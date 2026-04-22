package instancetype

import (
	"fmt"

	azurev1beta1 "github.com/Azure/karpenter-provider-azure/pkg/apis/v1beta1"
	azinstancetype "github.com/Azure/karpenter-provider-azure/pkg/providers/instancetype"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpcloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

const (
	// Architecture is amd64 only for Phase 1 (all five catalog SKUs are amd64).
	Architecture = "amd64"

	// DefaultPerNodePodsCount is used when AzureFlexNodeClass.Spec.MaxPodsPerNode is unset.
	DefaultPerNodePodsCount int32 = 110
)

// NodeClassKey holds the AzureFlexNodeClass fields that affect instance-type
// shape. We use it as a value-type so it's hashable for any future caching.
type NodeClassKey struct {
	Region           string
	OSDiskSizeGiB    int64
	PerNodePodsCount int32
}

// New builds a Karpenter InstanceType from a CatalogEntry, NodeClass key, and
// pre-built offerings. We follow the nebius shape so labels and overhead match.
func New(
	key NodeClassKey,
	entry *CatalogEntry,
	offerings karpcloudprovider.Offerings,
) *karpcloudprovider.InstanceType {
	return &karpcloudprovider.InstanceType{
		Name:         entry.Name,
		Requirements: requirements(key.Region, entry, offerings),
		Offerings:    offerings,
		Capacity:     capacity(key, entry),
		Overhead:     overhead(entry),
	}
}

func requirements(
	region string,
	e *CatalogEntry,
	offerings karpcloudprovider.Offerings,
) scheduling.Requirements {
	// Single zone (empty) for Phase 1: cross-region zonal placement is deferred.
	zones := []string{""}
	for _, o := range offerings {
		if zoneReq := o.Requirements.Get(corev1.LabelTopologyZone); zoneReq != nil {
			zones = zoneReq.Values()
		}
	}
	capacityTypes := []string{karpv1.CapacityTypeOnDemand}

	vCPU := fmt.Sprint(e.VCPU)
	memMiB := fmt.Sprint(e.MemoryGB * 1024)
	gpu := fmt.Sprint(e.GPU)

	return scheduling.NewRequirements(
		scheduling.NewRequirement(corev1.LabelInstanceTypeStable, corev1.NodeSelectorOpIn, e.Name),
		scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zones...),
		scheduling.NewRequirement(corev1.LabelTopologyRegion, corev1.NodeSelectorOpIn, region),
		scheduling.NewRequirement(corev1.LabelOSStable, corev1.NodeSelectorOpIn, string(corev1.Linux)),
		scheduling.NewRequirement(corev1.LabelArchStable, corev1.NodeSelectorOpIn, Architecture),
		scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, capacityTypes...),
		// Azure-domain labels (mirrors karpenter-provider-azure conventions).
		scheduling.NewRequirement(azurev1beta1.LabelSKUCPU, corev1.NodeSelectorOpIn, vCPU),
		scheduling.NewRequirement(azurev1beta1.LabelSKUMemory, corev1.NodeSelectorOpIn, memMiB),
		scheduling.NewRequirement(azurev1beta1.AKSLabelCPU, corev1.NodeSelectorOpIn, vCPU),
		scheduling.NewRequirement(azurev1beta1.AKSLabelMemory, corev1.NodeSelectorOpIn, memMiB),
		scheduling.NewRequirement(azurev1beta1.LabelSKUGPUCount, corev1.NodeSelectorOpIn, gpu),
	)
}

func capacity(key NodeClassKey, e *CatalogEntry) corev1.ResourceList {
	osDisk := *resource.NewScaledQuantity(key.OSDiskSizeGiB, resource.Giga)
	pods := resource.MustParse(fmt.Sprintf("%d", key.PerNodePodsCount))
	mem := resource.NewScaledQuantity(e.MemoryGB, resource.Giga)
	cpu := resource.NewQuantity(e.VCPU, resource.DecimalSI)
	gpu := resource.NewQuantity(e.GPU, resource.DecimalSI)
	return corev1.ResourceList{
		corev1.ResourceCPU:                    *cpu,
		corev1.ResourceMemory:                 *mem,
		corev1.ResourceEphemeralStorage:       osDisk,
		corev1.ResourcePods:                   pods,
		corev1.ResourceName("nvidia.com/gpu"): *gpu,
	}
}

func overhead(e *CatalogEntry) *karpcloudprovider.InstanceTypeOverhead {
	return &karpcloudprovider.InstanceTypeOverhead{
		KubeReserved:      azinstancetype.KubeReservedResources(e.VCPU, float64(e.MemoryGB)),
		SystemReserved:    azinstancetype.SystemReservedResources(),
		EvictionThreshold: azinstancetype.EvictionThreshold(),
	}
}
