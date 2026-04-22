package instancetype

import (
	corev1 "k8s.io/api/core/v1"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpcloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// defaultPrice is the placeholder price used for all Phase 1 catalog SKUs. The
// real price is irrelevant because consolidation is not enabled across cross-
// region NodePools — but Karpenter requires a non-zero price for ordering.
const defaultPrice = 1.0

// Offerings returns the cross-region offerings for a single SKU. Phase 1:
//   - on-demand only (no spot)
//   - empty zone (cross-region zonal placement is deferred)
func Offerings(region string) karpcloudprovider.Offerings {
	return karpcloudprovider.Offerings{
		{
			Price:     defaultPrice,
			Available: true,
			Requirements: scheduling.NewRequirements(
				scheduling.NewRequirement(karpv1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, karpv1.CapacityTypeOnDemand),
				// Empty zone string — Phase 1 is region-only. Karpenter requires
				// the zone requirement to exist on every offering.
				scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, ""),
			),
		},
	}
}
