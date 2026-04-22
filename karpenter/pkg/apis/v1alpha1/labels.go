package v1alpha1

import "github.com/Azure/aks-flex/karpenter/pkg/apis"

const (
	TerminationFinalizer = apis.Group + "/termination"

	// AzureFlexNodeClassHashAnnotation stores the deterministic hash of the
	// AzureFlexNodeClass spec at the time a NodeClaim was created. The
	// CloudProvider compares it against the current spec hash to compute drift.
	AzureFlexNodeClassHashAnnotation = apis.Group + "/azureflex-nodeclass-hash"
)
