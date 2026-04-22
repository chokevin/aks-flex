package azure

import (
	"testing"

	"github.com/Azure/aks-flex/karpenter/pkg/apis/v1alpha1"
)

func TestValidateSpec(t *testing.T) {
	good := v1alpha1.AzureFlexNodeClassSpec{
		SubscriptionID: "sub",
		Location:       "eastus2",
		ResourceGroup:  "rg",
		SubnetID:       "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/v/subnets/s",
	}
	if err := validateSpec(good); err != nil {
		t.Fatalf("expected good spec to pass: %v", err)
	}

	bad := []v1alpha1.AzureFlexNodeClassSpec{
		{Location: "eastus2", ResourceGroup: "rg", SubnetID: "/subscriptions/x"}, // missing sub
		{SubscriptionID: "sub", ResourceGroup: "rg", SubnetID: "/subscriptions/x"}, // missing loc
		{SubscriptionID: "sub", Location: "eastus2", SubnetID: "/subscriptions/x"}, // missing rg
		{SubscriptionID: "sub", Location: "eastus2", ResourceGroup: "rg", SubnetID: "not-an-arm-id"},
	}
	for i, s := range bad {
		if err := validateSpec(s); err == nil {
			t.Errorf("case %d: expected validateSpec to fail", i)
		}
	}

	// Mutually exclusive fields.
	id := "/subscriptions/sub/.../images/x"
	mut := good
	mut.ImageReference = &v1alpha1.AzureFlexImageReference{Publisher: "p", Offer: "o", SKU: "s"}
	mut.ImageID = &id
	if err := validateSpec(mut); err == nil {
		t.Fatalf("imageReference + imageID must be mutually exclusive")
	}
}
