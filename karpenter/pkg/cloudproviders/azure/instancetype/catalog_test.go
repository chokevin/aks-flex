package instancetype

import "testing"

func TestCatalogContainsRequiredSKUs(t *testing.T) {
	required := []string{
		"Standard_ND96isr_H200_v5",
		"Standard_ND96amsr_A100_v4",
		"Standard_NC40ads_H100_v5",
		"Standard_NC24ads_A100_v4",
		"Standard_D8s_v5",
	}
	for _, name := range required {
		if Get(name) == nil {
			t.Errorf("catalog missing required SKU %q", name)
		}
	}
}

func TestCatalogGetUnknown(t *testing.T) {
	if Get("Standard_DoesNotExist_v1") != nil {
		t.Fatalf("Get must return nil for unknown SKU")
	}
}

func TestProviderResolveUnknownSKU(t *testing.T) {
	// Bypass NodeClaim machinery: an instance type name not in the catalog
	// must come back as a clean error from the provider. We exercise this
	// path indirectly via GetByName since ResolveFromNodeClaim requires
	// scheduling fixtures.
	p := NewProvider()
	if p.GetByName(NodeClassKey{Region: "eastus2"}, "Standard_DoesNotExist_v1") != nil {
		t.Fatalf("GetByName must return nil for unknown SKU")
	}
}

func TestProviderListCount(t *testing.T) {
	p := NewProvider()
	its := p.GetInstanceTypes(NodeClassKey{Region: "eastus2", OSDiskSizeGiB: 128, PerNodePodsCount: 110})
	if len(its) != len(Catalog) {
		t.Fatalf("GetInstanceTypes returned %d entries, want %d", len(its), len(Catalog))
	}
}
