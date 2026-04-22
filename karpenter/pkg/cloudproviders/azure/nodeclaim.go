package azure

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/Azure/karpenter-provider-azure/pkg/operator/options"
	labelspkg "github.com/Azure/karpenter-provider-azure/pkg/providers/labels"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/utils/resources"

	stretchapi "github.com/Azure/aks-flex/plugin/api"
	"github.com/Azure/aks-flex/plugin/pkg/services/agentpools/api/features/kubeadm"
	flexvm "github.com/Azure/aks-flex/plugin/pkg/services/agentpools/azure/flexvm"
	"github.com/Azure/aks-flex/plugin/pkg/topology"

	"github.com/Azure/aks-flex/karpenter/pkg/apis/v1alpha1"
	"github.com/Azure/aks-flex/karpenter/pkg/cloudproviders"
)

// providerID format:
//
//   azure-flex:///subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Compute/virtualMachines/<name>
//
// Three slashes after the scheme: empty host, then the canonical ARM resource
// id (which starts with a slash). Round-trip via [providerIDToARMID] /
// [armIDToProviderID] is lossless.

func armIDToProviderID(armID string) string {
	if !strings.HasPrefix(armID, "/") {
		armID = "/" + armID
	}
	return ProviderIDScheme + "://" + armID
}

func providerIDToARMID(providerID string) (string, error) {
	u, err := url.Parse(providerID)
	if err != nil {
		return "", fmt.Errorf("parsing providerID %q: %w", providerID, err)
	}
	if u.Scheme != ProviderIDScheme {
		return "", fmt.Errorf("unexpected providerID scheme %q, expected %q", u.Scheme, ProviderIDScheme)
	}
	if u.Host != "" {
		// Canonical form has empty host. If there's anything in the host
		// position the providerID was constructed wrong.
		return "", fmt.Errorf("providerID %q has unexpected host %q", providerID, u.Host)
	}
	if u.Path == "" {
		return "", fmt.Errorf("providerID %q has empty ARM path", providerID)
	}
	return u.Path, nil
}

// providerIDToVMName extracts the VM name (last path segment) from the providerID.
func providerIDToVMName(providerID string) (string, error) {
	armID, err := providerIDToARMID(providerID)
	if err != nil {
		return "", err
	}
	parts := strings.Split(strings.TrimPrefix(armID, "/"), "/")
	if len(parts) == 0 {
		return "", fmt.Errorf("providerID %q has no name segment", providerID)
	}
	return parts[len(parts)-1], nil
}

// agentPoolToNodeClaim rebuilds a karpenter NodeClaim from a flexvm AgentPool
// returned by the plugin. The instanceType supplies the well-known scheduling
// labels and capacity.
func agentPoolToNodeClaim(
	ap *flexvm.AgentPool,
	instanceType *cloudprovider.InstanceType,
) *v1.NodeClaim {
	rv := &v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:              ap.GetMetadata().GetId(),
			Labels:            map[string]string{},
			Annotations:       map[string]string{},
			CreationTimestamp: metav1.NewTime(ap.GetStatus().GetCreatedAt().AsTime()),
		},
		Spec: v1.NodeClaimSpec{},
		Status: v1.NodeClaimStatus{
			ProviderID: armIDToProviderID(ap.GetStatus().GetVmResourceId()),
		},
	}

	if instanceType != nil {
		rv.Labels = labelspkg.GetAllSingleValuedRequirementLabels(instanceType.Requirements)
		rv.Status.Capacity = lo.PickBy(instanceType.Capacity, filterNonZero)
		rv.Status.Allocatable = lo.PickBy(instanceType.Allocatable(), filterNonZero)
	}

	// Phase 1: on-demand only, region-only.
	rv.Labels[v1.CapacityTypeLabelKey] = v1.CapacityTypeOnDemand
	rv.Labels[corev1.LabelTopologyRegion] = ap.GetSpec().GetLocation()

	return rv
}

// nodeClaimToAgentPool builds the plugin AgentPool message from the karpenter
// NodeClass + NodeClaim + resolved instance type. Resource names are entirely
// deterministic from the NodeClaim name (for retry idempotency).
func nodeClaimToAgentPool(
	karpOpts *options.Options,
	clusterCA []byte,
	nodeClass *v1alpha1.AzureFlexNodeClass,
	nodeClaim *v1.NodeClaim,
	instanceType *cloudprovider.InstanceType,
) *flexvm.AgentPool {
	mdBuilder := stretchapi.Metadata_builder{
		Id: lo.ToPtr(nodeClaim.Name),
	}

	osDiskSize := lo.FromPtrOr(nodeClass.Spec.OSDiskSizeGB, 128)
	securityType := lo.FromPtrOr(nodeClass.Spec.SecurityType, "Standard")

	kubeadmConfig := kubeadm.Config_builder{
		Server:                   lo.ToPtr(karpOpts.ClusterEndpoint),
		CertificateAuthorityData: clusterCA,
		Token:                    lo.ToPtr(karpOpts.KubeletClientTLSBootstrapToken),
		NodeLabels: map[string]string{
			cloudproviders.NodeClaimLabelKey:          nodeClaim.Name,
			topology.NodeLabelKeyCloudProviderManaged: "false",
			topology.NodeLabelKeyCloudProviderCluster: karpOpts.NodeResourceGroup,
			topology.NodeLabelKeyStretchManaged:       "true",
		},
	}.Build()
	kubeadmConfig.AddNodeLabels(map[string]string{
		corev1.LabelInstanceTypeStable: instanceType.Name,
		corev1.LabelTopologyRegion:     nodeClass.Spec.Location,
		// Empty zone — region-only Phase 1.
		corev1.LabelTopologyZone:  "",
		v1.CapacityTypeLabelKey:   v1.CapacityTypeOnDemand,
		"kubernetes.azure.com/mode": "user",
	})
	kubeadmConfig.AddK8SRegisterTaints(v1.UnregisteredNoExecuteTaint)

	specBuilder := flexvm.AgentPoolSpec_builder{
		SubscriptionId:   lo.ToPtr(nodeClass.Spec.SubscriptionID),
		ResourceGroup:    lo.ToPtr(nodeClass.Spec.ResourceGroup),
		Location:         lo.ToPtr(nodeClass.Spec.Location),
		SubnetId:         lo.ToPtr(nodeClass.Spec.SubnetID),
		VmSize:           lo.ToPtr(instanceType.Name),
		SecurityType:     lo.ToPtr(securityType),
		OsDiskSizeGb:     lo.ToPtr(int32(osDiskSize)),
		SshPublicKeys:    nodeClass.Spec.SSHPublicKeys,
		AllocatePublicIp: lo.ToPtr(lo.FromPtrOr(nodeClass.Spec.AllocateNodePublicIP, false)),
		Tags:             nodeClass.Spec.Tags,
		Kubeadm:          kubeadmConfig,
	}
	if ref := nodeClass.Spec.ImageReference; ref != nil {
		specBuilder.ImageReference = flexvm.ImageReference_builder{
			Publisher: lo.ToPtr(ref.Publisher),
			Offer:     lo.ToPtr(ref.Offer),
			Sku:       lo.ToPtr(ref.SKU),
			Version:   lo.ToPtr(ref.Version),
		}.Build()
	}
	if id := lo.FromPtrOr(nodeClass.Spec.ImageID, ""); id != "" {
		specBuilder.ImageId = lo.ToPtr(id)
	}

	return flexvm.AgentPool_builder{
		Metadata: mdBuilder.Build(),
		Spec:     specBuilder.Build(),
	}.Build()
}

// driftHash returns a deterministic hex digest over the AzureFlexNodeClass
// fields whose change must trigger node drift. Mirrors the nebius "rebuild
// from spec" pattern.
func driftHash(spec v1alpha1.AzureFlexNodeClassSpec) string {
	h := sha256.New()
	write := func(s string) { _, _ = h.Write([]byte(s)); _, _ = h.Write([]byte{0}) }

	write(spec.SubscriptionID)
	write(spec.Location)
	write(spec.ResourceGroup)
	write(spec.SubnetID)
	write(lo.FromPtrOr(spec.SecurityType, "Standard"))
	write(fmt.Sprintf("%d", lo.FromPtrOr(spec.OSDiskSizeGB, 128)))
	if ref := spec.ImageReference; ref != nil {
		write("imgref:" + ref.Publisher + "|" + ref.Offer + "|" + ref.SKU + "|" + ref.Version)
	} else {
		write("imgref:")
	}
	write("imgid:" + lo.FromPtrOr(spec.ImageID, ""))
	// Tags affect downstream observability/billing but not the VM identity.
	// They DO contribute to drift so an operator-driven tag rotation forces
	// nodes to reconcile.
	keys := make([]string, 0, len(spec.Tags))
	for k := range spec.Tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		write("tag:" + k + "=" + spec.Tags[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

func filterNonZero(_ corev1.ResourceName, q resource.Quantity) bool {
	return !resources.IsZero(q)
}
