package instancetype

import (
	"fmt"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpcloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// Provider returns InstanceTypes for a given AzureFlexNodeClass key. There's
// no caching or background refresh because the Phase 1 catalog is hardcoded.
type Provider struct{}

func NewProvider() *Provider { return &Provider{} }

// GetInstanceTypes returns one InstanceType per catalog entry, all rooted at
// the NodeClass's region. Order matches Catalog (stable across calls).
func (p *Provider) GetInstanceTypes(key NodeClassKey) []*karpcloudprovider.InstanceType {
	offerings := Offerings(key.Region)
	out := make([]*karpcloudprovider.InstanceType, 0, len(Catalog))
	for i := range Catalog {
		out = append(out, New(key, &Catalog[i], offerings))
	}
	return out
}

// GetByName returns a single InstanceType by SKU name, or nil if the SKU is
// not in the Phase 1 catalog.
func (p *Provider) GetByName(key NodeClassKey, name string) *karpcloudprovider.InstanceType {
	entry := Get(name)
	if entry == nil {
		return nil
	}
	return New(key, entry, Offerings(key.Region))
}

// ResolveFromNodeClaim picks the catalog SKU that matches the NodeClaim's
// requirements. Phase 1 chooses the first matching catalog entry by stable
// catalog order (price is uniform). Returns an error if no catalog SKU
// satisfies the requirements.
func (p *Provider) ResolveFromNodeClaim(
	key NodeClassKey,
	requirements []karpv1.NodeSelectorRequirementWithMinValues,
) (*karpcloudprovider.InstanceType, error) {
	reqs := scheduling.NewNodeSelectorRequirementsWithMinValues(requirements...)

	requested := map[string]struct{}{}
	if itReq := reqs.Get(corev1.LabelInstanceTypeStable); itReq != nil {
		for _, v := range itReq.Values() {
			requested[v] = struct{}{}
		}
	}

	for i := range Catalog {
		e := &Catalog[i]
		if len(requested) > 0 {
			if _, ok := requested[e.Name]; !ok {
				continue
			}
		}
		it := New(key, e, Offerings(key.Region))
		if !reqs.IsCompatible(it.Requirements, scheduling.AllowUndefinedWellKnownLabels) {
			continue
		}
		return it, nil
	}

	return nil, fmt.Errorf("no AzureFlex catalog SKU matches NodeClaim requirements (requested=%v)",
		lo.Keys(requested))
}
