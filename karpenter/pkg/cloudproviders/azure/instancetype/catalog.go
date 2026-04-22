package instancetype

// CatalogEntry is a Phase 1 hardcoded SKU description. We deliberately do NOT
// call Azure's SKU/quota APIs from the karpenter controller — letting ARM
// fail and classifying the error is simpler and correct for Phase 1 (issue #63).
//
// Future work: replace this with a dynamic provider backed by armcompute's
// resourceSkus client (and per-region offering refresh).
type CatalogEntry struct {
	// Name is the Azure VM size, used as both the karpenter instance type
	// name and the Azure VMSize when creating the VM.
	Name string

	VCPU     int64
	MemoryGB int64
	GPU      int64
}

// Catalog is the hardcoded allowlist of SKUs that AzureFlexNodeClass
// NodePools may schedule onto in Phase 1.
var Catalog = []CatalogEntry{
	{Name: "Standard_ND96isr_H200_v5", VCPU: 96, MemoryGB: 1900, GPU: 8},
	{Name: "Standard_ND96amsr_A100_v4", VCPU: 96, MemoryGB: 1900, GPU: 8},
	{Name: "Standard_NC40ads_H100_v5", VCPU: 40, MemoryGB: 320, GPU: 1},
	{Name: "Standard_NC24ads_A100_v4", VCPU: 24, MemoryGB: 220, GPU: 1},
	{Name: "Standard_D8s_v5", VCPU: 8, MemoryGB: 32, GPU: 0},
}

// Get returns the CatalogEntry for name, or nil if name is not in the catalog.
func Get(name string) *CatalogEntry {
	for i := range Catalog {
		if Catalog[i].Name == name {
			return &Catalog[i]
		}
	}
	return nil
}
