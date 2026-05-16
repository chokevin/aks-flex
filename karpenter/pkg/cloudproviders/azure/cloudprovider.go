// Package azure implements the AzureFlex cross-region cloud provider for
// Karpenter. It is distinct from the upstream Azure/karpenter-provider-azure
// (which only supports VMs in the same Azure region as the AKS cluster) and
// from the in-tree AKS provider wired up alongside it in cmd/controller/main.go.
//
// The provider talks to a colocated plugin gRPC service (flexvm) that performs
// the actual Azure API calls. This isolates the Karpenter controller from
// Azure SDK details and lets the plugin run with its own (Contributor-scoped)
// managed identity.
package azure

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	karpoptions "github.com/Azure/karpenter-provider-azure/pkg/operator/options"
	"github.com/Azure/karpenter-provider-azure/pkg/utils"
	"github.com/awslabs/operatorpkg/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"

	pluginapi "github.com/Azure/aks-flex/plugin/api"
	stretchhelper "github.com/Azure/aks-flex/plugin/pkg/helper"
	stretchservices "github.com/Azure/aks-flex/plugin/pkg/services"
	agentpoolsapi "github.com/Azure/aks-flex/plugin/pkg/services/agentpools/api"
	flexvm "github.com/Azure/aks-flex/plugin/pkg/services/agentpools/azure/flexvm"

	"github.com/Azure/aks-flex/karpenter/pkg/apis"
	"github.com/Azure/aks-flex/karpenter/pkg/apis/v1alpha1"
	"github.com/Azure/aks-flex/karpenter/pkg/cloudproviders"
	"github.com/Azure/aks-flex/karpenter/pkg/cloudproviders/azure/instancetype"
)

const incompleteAgentPoolCleanupDelay = 30 * time.Minute

type CloudProvider struct {
	stretchPluginConn       *grpc.ClientConn
	stretchAgentPoolsClient agentpoolsapi.AgentPoolsClient

	kubeClient client.Client

	// clusterCA is captured once at startup. Per the rubber-duck note, the
	// AKS bootstrap secret lookup (kubeadm.FromAKS) is "exactly one secret per
	// call", so anything cluster-wide we need is captured at struct init —
	// we do not re-fetch on every Create.
	clusterCA []byte

	instanceTypeProvider *instancetype.Provider

	cleanupInFlight sync.Map
}

var flexAgentPoolTypeURL = "type.googleapis.com/" + string((&flexvm.AgentPool{}).ProtoReflect().Descriptor().FullName())

func newCloudProvider(
	stretchPluginConn *grpc.ClientConn,
	kubeClient client.Client,
	clusterCA []byte,
) *CloudProvider {
	return &CloudProvider{
		stretchPluginConn:       stretchPluginConn,
		stretchAgentPoolsClient: agentpoolsapi.NewAgentPoolsClient(stretchPluginConn),
		kubeClient:              kubeClient,
		clusterCA:               clusterCA,
		instanceTypeProvider:    instancetype.NewProvider(),
	}
}

// Register installs the AzureFlex provider into the multiplexing hub.
func Register(
	ctx context.Context,
	hub *cloudproviders.CloudProvidersHub,
	kubeClient client.Client,
	clusterCA []byte,
) error {
	stretchPluginConn, err := stretchservices.NewConnection()
	if err != nil {
		return fmt.Errorf("creating stretch plugin connection: %w", err)
	}
	cp := newCloudProvider(stretchPluginConn, kubeClient, clusterCA)
	hub.Register(cp, GroupKind, ProviderIDScheme)
	return nil
}

var _ corecloudprovider.CloudProvider = (*CloudProvider)(nil)

func (c *CloudProvider) getNodeClass(
	ctx context.Context,
	ref *v1.NodeClassReference,
) (*v1alpha1.AzureFlexNodeClass, error) {
	if ref == nil {
		return nil, errors.New("nodeClaim must reference a node class")
	}
	if ref.Group != apis.Group {
		return nil, fmt.Errorf("nodeClassRef %s in group %q, expected %q", ref.Name, ref.Group, apis.Group)
	}

	rv := &v1alpha1.AzureFlexNodeClass{}
	if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: ref.Name}, rv); err != nil {
		return nil, fmt.Errorf("getting AzureFlexNodeClass %s: %w", ref.Name, err)
	}
	if !rv.DeletionTimestamp.IsZero() {
		return nil, utils.NewTerminatingResourceError(
			schema.GroupResource{Group: apis.Group, Resource: "azureflexnodeclasses"},
			rv.Name,
		)
	}
	return rv, nil
}

func (c *CloudProvider) instanceTypeKey(nc *v1alpha1.AzureFlexNodeClass) instancetype.NodeClassKey {
	osDisk := int64(128)
	if nc.Spec.OSDiskSizeGB != nil {
		osDisk = int64(*nc.Spec.OSDiskSizeGB)
	}
	pods := instancetype.DefaultPerNodePodsCount
	if nc.Spec.MaxPodsPerNode != nil {
		pods = *nc.Spec.MaxPodsPerNode
	}
	return instancetype.NodeClassKey{
		Region:           nc.Spec.Location,
		OSDiskSizeGiB:    osDisk,
		PerNodePodsCount: pods,
	}
}

func (c *CloudProvider) Create(ctx context.Context, nodeClaim *v1.NodeClaim) (*v1.NodeClaim, error) {
	logger := log.FromContext(ctx).WithValues("nodeClaim", nodeClaim.Name)
	logger.Info("creating azure-flex VM for nodeClaim")

	nodeClass, err := c.getNodeClass(ctx, nodeClaim.Spec.NodeClassRef)
	if err != nil {
		return nil, err
	}

	key := c.instanceTypeKey(nodeClass)
	it, err := c.instanceTypeProvider.ResolveFromNodeClaim(key, nodeClaim.Spec.Requirements)
	if err != nil {
		// Schedule-time error: the requested SKU isn't in the Phase 1 catalog.
		return nil, corecloudprovider.NewInsufficientCapacityError(err)
	}
	logger.Info("resolved instance type", "instanceType", it.Name)

	agentPool := nodeClaimToAgentPool(
		karpoptions.FromContext(ctx),
		c.clusterCA,
		nodeClass,
		nodeClaim,
		it,
	)
	created, err := stretchhelper.CreateOrUpdate(
		c.stretchAgentPoolsClient.CreateOrUpdate,
		ctx, agentPool,
	)
	if err != nil {
		if IsQuotaError(err) {
			c.cleanupAgentPoolInBackground(ctx, nodeClaim.Name, "quota/capacity create failure")
			return nil, corecloudprovider.NewInsufficientCapacityError(err)
		}
		return nil, fmt.Errorf("creating azure-flex agent pool: %w", err)
	}

	// Stamp the NodeClass drift hash onto the returned NodeClaim so that
	// IsDrifted can detect spec changes later. Without this annotation the
	// drift check silently no-ops.
	out := agentPoolToNodeClaim(created, it)
	if out.Annotations == nil {
		out.Annotations = map[string]string{}
	}
	out.Annotations[v1alpha1.AzureFlexNodeClassHashAnnotation] = driftHash(nodeClass.Spec)
	return out, nil
}

func (c *CloudProvider) Delete(ctx context.Context, nodeClaim *v1.NodeClaim) error {
	return c.deleteAgentPool(ctx, nodeClaim.Name)
}

func (c *CloudProvider) deleteAgentPool(ctx context.Context, name string) error {
	logger := log.FromContext(ctx).WithValues("agentPool", name)
	// Per CloudProvider.Delete contract: signal NodeClaimNotFoundError if the
	// remote resource is already gone (so karpenter knows it's safe to drop).
	if _, err := c.getFlexAgentPool(ctx, name); err != nil {
		if IsNotFound(err) || IsTypeMismatch(err) {
			return corecloudprovider.NewNodeClaimNotFoundError(err)
		}
		// Non-NotFound get failure: log and proceed with delete in best effort.
		logger.V(5).Error(err, "getting agent pool for nodeClaim, proceeding to delete")
	}

	if err := stretchhelper.Delete(
		c.stretchAgentPoolsClient.Delete,
		ctx, name,
	); err != nil {
		if IsNotFound(err) || IsTypeMismatch(err) {
			return corecloudprovider.NewNodeClaimNotFoundError(err)
		}
		return fmt.Errorf("deleting azure-flex agent pool: %w", err)
	}
	logger.Info("deleted azure-flex agent pool")
	return nil
}

func (c *CloudProvider) Get(ctx context.Context, providerID string) (*v1.NodeClaim, error) {
	name, err := providerIDToVMName(providerID)
	if err != nil {
		return nil, err
	}
	ap, err := c.getFlexAgentPool(ctx, name)
	if err != nil {
		if IsNotFound(err) || IsTypeMismatch(err) {
			return nil, corecloudprovider.NewNodeClaimNotFoundError(err)
		}
		return nil, err
	}
	// We don't have the NodeClass here (Get is called by reconcilers that may
	// not have a class on hand) — pass nil instanceType and accept missing
	// well-known labels. They'll be repopulated by the next Create-flow Get.
	return agentPoolToNodeClaim(ap, nil), nil
}

func (c *CloudProvider) getFlexAgentPool(ctx context.Context, id string) (*flexvm.AgentPool, error) {
	req := &pluginapi.GetRequest{}
	req.SetId(id)
	resp, err := c.stretchAgentPoolsClient.Get(ctx, req)
	if err != nil {
		return nil, err
	}
	return flexAgentPoolFromGetResponse(resp)
}

func flexAgentPoolFromGetResponse(resp *pluginapi.GetResponse) (*flexvm.AgentPool, error) {
	if resp == nil || resp.GetItem() == nil {
		return nil, grpcstatus.Error(codes.NotFound, "")
	}
	if resp.GetItem().GetTypeUrl() != flexAgentPoolTypeURL {
		return nil, grpcstatus.Error(codes.NotFound, "")
	}
	return stretchhelper.AnyTo[*flexvm.AgentPool](resp.GetItem())
}

func (c *CloudProvider) List(ctx context.Context) ([]*v1.NodeClaim, error) {
	aps, err := stretchhelper.ListByType[*flexvm.AgentPool](
		c.stretchAgentPoolsClient.List,
		ctx, "",
	)
	if err != nil {
		return nil, err
	}
	out := make([]*v1.NodeClaim, 0, len(aps))
	now := time.Now()
	for _, ap := range aps {
		if ap.GetStatus().GetVmResourceId() == "" {
			if shouldCleanupIncompleteAgentPool(ap, now) {
				c.cleanupAgentPoolInBackground(ctx, ap.GetMetadata().GetId(), "stale incomplete agent pool")
			}
			continue
		}
		out = append(out, agentPoolToNodeClaim(ap, nil))
	}
	return out, nil
}

func shouldCleanupIncompleteAgentPool(ap *flexvm.AgentPool, now time.Time) bool {
	if ap.GetStatus().GetVmResourceId() != "" {
		return false
	}
	createdAt := ap.GetStatus().GetCreatedAt()
	if createdAt == nil {
		return true
	}
	return !createdAt.AsTime().Add(incompleteAgentPoolCleanupDelay).After(now)
}

func (c *CloudProvider) cleanupAgentPoolInBackground(ctx context.Context, name, reason string) {
	if name == "" {
		return
	}
	if _, loaded := c.cleanupInFlight.LoadOrStore(name, struct{}{}); loaded {
		return
	}

	logger := log.FromContext(ctx).WithValues("agentPool", name, "reason", reason)
	logger.Info("starting azure-flex agent pool cleanup")
	go func() {
		defer c.cleanupInFlight.Delete(name)

		cleanupCtx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
		defer cancel()

		if err := c.deleteAgentPool(cleanupCtx, name); err != nil && !corecloudprovider.IsNodeClaimNotFoundError(err) {
			logger.Error(err, "cleaning up azure-flex agent pool")
			return
		}
		logger.Info("cleaned up azure-flex agent pool")
	}()
}

func (c *CloudProvider) GetInstanceTypes(ctx context.Context, nodePool *v1.NodePool) ([]*corecloudprovider.InstanceType, error) {
	logger := loggerFromContext(ctx).WithValues("nodePool", nodePool.Name)

	nodeClass, err := c.getNodeClass(ctx, nodePool.Spec.Template.Spec.NodeClassRef)
	if err != nil {
		return nil, fmt.Errorf("getting node class for node pool: %w", err)
	}

	its := c.instanceTypeProvider.GetInstanceTypes(c.instanceTypeKey(nodeClass))
	logger.V(5).Info("listed instance types", "count", len(its))
	return its, nil
}

func (c *CloudProvider) GetSupportedNodeClasses() []status.Object {
	return []status.Object{
		&v1alpha1.AzureFlexNodeClass{},
	}
}

func (c *CloudProvider) IsDrifted(ctx context.Context, nodeClaim *v1.NodeClaim) (corecloudprovider.DriftReason, error) {
	if nodeClaim.Spec.NodeClassRef == nil {
		return "", nil
	}
	nc := &v1alpha1.AzureFlexNodeClass{}
	if err := c.kubeClient.Get(ctx, client.ObjectKey{Name: nodeClaim.Spec.NodeClassRef.Name}, nc); err != nil {
		return "", client.IgnoreNotFound(err)
	}

	current := driftHash(nc.Spec)
	prior := nodeClaim.Annotations[v1alpha1.AzureFlexNodeClassHashAnnotation]
	if prior != "" && prior != current {
		return corecloudprovider.DriftReason("AzureFlexNodeClassChanged"), nil
	}
	return "", nil
}

func (c *CloudProvider) Name() string {
	return ProviderIDScheme
}

func (c *CloudProvider) RepairPolicies() []corecloudprovider.RepairPolicy {
	return []corecloudprovider.RepairPolicy{}
}

func (c *CloudProvider) Close(context.Context) error {
	if c.stretchPluginConn != nil {
		return c.stretchPluginConn.Close()
	}
	return nil
}
