package azure

import (
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/Azure/aks-flex/karpenter/pkg/apis"
)

const (
	// ProviderIDScheme is the URL scheme used in NodeClaim.Status.ProviderID
	// for instances managed by the Azure cross-region (flex) cloud provider.
	//
	// Distinct from "azure" (which the AKS in-region provider uses) so the
	// Karpenter cloud-provider hub can multiplex correctly.
	ProviderIDScheme = "azure-flex"
)

var GroupKind = schema.GroupKind{
	Group: apis.Group,
	Kind:  "AzureFlexNodeClass",
}
