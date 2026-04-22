package azure

import (
	"strings"
	"testing"

	"github.com/Azure/aks-flex/karpenter/pkg/apis/v1alpha1"
)

func TestProviderIDRoundTrip(t *testing.T) {
	armID := "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/my-rg/providers/Microsoft.Compute/virtualMachines/nodeclaim-abc"
	pid := armIDToProviderID(armID)

	if !strings.HasPrefix(pid, "azure-flex:///subscriptions/") {
		t.Fatalf("providerID %q must have azure-flex:/// prefix and a slash before subscriptions", pid)
	}

	got, err := providerIDToARMID(pid)
	if err != nil {
		t.Fatalf("providerIDToARMID: %v", err)
	}
	if got != armID {
		t.Fatalf("round-trip mismatch:\n  in:  %s\n  out: %s", armID, got)
	}

	name, err := providerIDToVMName(pid)
	if err != nil {
		t.Fatalf("providerIDToVMName: %v", err)
	}
	if name != "nodeclaim-abc" {
		t.Fatalf("expected name nodeclaim-abc, got %s", name)
	}
}

func TestProviderIDInvalidScheme(t *testing.T) {
	cases := []string{
		"aks-nebius://abc",
		"https://example.com/foo",
		"azure:///subscriptions/x/y",
		"not-a-url",
	}
	for _, c := range cases {
		t.Run(c, func(t *testing.T) {
			if _, err := providerIDToARMID(c); err == nil {
				t.Fatalf("expected error parsing providerID %q", c)
			}
		})
	}
}

func TestProviderIDRejectsHost(t *testing.T) {
	// Three slashes are required: azure-flex:///<arm-id>. Two slashes followed
	// by something puts that something in the URL host, which we reject.
	bad := "azure-flex://hostname/subscriptions/x/y"
	if _, err := providerIDToARMID(bad); err == nil {
		t.Fatalf("expected error for providerID with host segment")
	}
}

func TestDriftHashDeterministic(t *testing.T) {
	mk := func() v1alpha1.AzureFlexNodeClassSpec {
		size := int32(256)
		sec := "Standard"
		return v1alpha1.AzureFlexNodeClassSpec{
			SubscriptionID: "sub",
			Location:       "eastus2",
			ResourceGroup:  "rg",
			SubnetID:       "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/v/subnets/s",
			SecurityType:   &sec,
			OSDiskSizeGB:   &size,
			Tags:           map[string]string{"a": "1", "b": "2"},
		}
	}
	a := driftHash(mk())
	b := driftHash(mk())
	if a != b {
		t.Fatalf("hash should be deterministic: %s != %s", a, b)
	}

	// Tags in different insertion order should yield the same hash because
	// driftHash sorts tag keys. Map iteration order is non-deterministic, so
	// build with the same content and just verify equality is preserved.
	c := mk()
	c.Tags = map[string]string{"b": "2", "a": "1"}
	if driftHash(c) != a {
		t.Fatalf("hash must not depend on map insertion order")
	}

	// Different subnet → different hash.
	d := mk()
	d.SubnetID = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/v/subnets/other"
	if driftHash(d) == a {
		t.Fatalf("different subnet should produce different hash")
	}
}
