package v1alpha1

import (
	"github.com/awslabs/operatorpkg/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AzureFlexNodeClass is the Schema for the AzureFlexNodeClass API.
//
// It enables a NodePool in an AKS cluster to auto-provision external Azure VMs in a
// (potentially different) Azure region than the AKS cluster's own region. Each node
// is a single VM (not VMSS) so that cross-region placement is straightforward.
//
// +kubebuilder:object:root=true
// +kubebuilder:resource:path=azureflexnodeclasses,scope=Cluster,shortName={afnc,afncs},categories={karpenter,nap}
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=".metadata.creationTimestamp"
// +kubebuilder:storageversion
// +kubebuilder:subresource:status
type AzureFlexNodeClass struct {
	metav1.TypeMeta `json:",inline"`
	// metadata is standard object metadata.
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`

	// +optional
	Spec AzureFlexNodeClassSpec `json:"spec,omitempty"`

	// status contains the resolved state of the AzureFlexNodeClass.
	// +optional
	Status AzureFlexNodeClassStatus `json:"status,omitempty"`
}

var _ status.Object = (*AzureFlexNodeClass)(nil)

func (s *AzureFlexNodeClass) GetConditions() []status.Condition {
	return s.Status.Conditions
}

func (s *AzureFlexNodeClass) SetConditions(conditions []status.Condition) {
	s.Status.Conditions = conditions
}

func (s *AzureFlexNodeClass) StatusConditions() status.ConditionSet {
	conds := []string{
		ConditionTypeValidationSucceeded,
	}
	return status.NewReadyConditions(conds...).For(s)
}

// AzureFlexImageReference selects an Azure Marketplace image.
// Mutually exclusive with AzureFlexNodeClassSpec.ImageID.
type AzureFlexImageReference struct {
	// +required
	Publisher string `json:"publisher"`
	// +required
	Offer string `json:"offer"`
	// +required
	SKU string `json:"sku"`
	// +optional
	// +default="latest"
	Version string `json:"version,omitempty"`
}

// AzureFlexNodeClassSpec is the spec for AzureFlexNodeClass.
//
// Phase 1 scope (issue #63): single region per NodeClass, no spot, no zones,
// no identity/UAMI per-NodeClass (the controller MI is assumed to have
// Contributor on the target subscription/RG/subnet), no quota preflight,
// no PPG/capacity reservation, no spot, no WireGuard.
type AzureFlexNodeClassSpec struct {
	// SubscriptionID is the Azure subscription where VMs will be created.
	// +required
	SubscriptionID string `json:"subscriptionID"`

	// Location is the Azure region (e.g. "eastus2"). May differ from the AKS cluster region.
	// +required
	Location string `json:"location"`

	// ResourceGroup is the resource group where VMs, NICs, and OS disks land.
	// Must already exist.
	// +required
	ResourceGroup string `json:"resourceGroup"`

	// SubnetID is the full ARM resource ID of the subnet (must already exist
	// and be reachable from the AKS cluster).
	// +required
	SubnetID string `json:"subnetID"`

	// ImageReference selects an Azure Marketplace image. Mutually exclusive with ImageID.
	// If neither is set, defaults to microsoft-dsvm/ubuntu-hpc/2204/latest.
	// +optional
	ImageReference *AzureFlexImageReference `json:"imageReference,omitempty"`

	// ImageID is a SIG / community gallery image resource ID. Mutually exclusive with ImageReference.
	// +optional
	ImageID *string `json:"imageID,omitempty"`

	// SecurityType selects the VM security profile. Currently only "Standard" is supported.
	// TrustedLaunch is deferred — it has been observed to break the DSVM image.
	// +optional
	// +default="Standard"
	// +kubebuilder:validation:Enum=Standard
	SecurityType *string `json:"securityType,omitempty"`

	// OSDiskSizeGB is the size of the OS disk in GB.
	// +optional
	// +default=128
	OSDiskSizeGB *int32 `json:"osDiskSizeGB,omitempty"`

	// SSHPublicKeys is the list of SSH public keys to install on each node.
	// +optional
	SSHPublicKeys []string `json:"sshPublicKeys,omitempty"`

	// AllocateNodePublicIP controls whether each node receives a public IP.
	// +optional
	// +default=false
	AllocateNodePublicIP *bool `json:"allocateNodePublicIP,omitempty"`

	// MaxPodsPerNode is advertised in the node's capacity and affects Karpenter scheduling.
	// +optional
	// +default=110
	MaxPodsPerNode *int32 `json:"maxPodsPerNode,omitempty"`

	// Tags are applied to every Azure resource (VM, NIC, OS disk) created from this NodeClass.
	// +optional
	Tags map[string]string `json:"tags,omitempty"`
}

type AzureFlexNodeClassStatus struct {
	// conditions contains signals for health and readiness
	// +optional
	//nolint:kubeapilinter // conditions: using status.Condition from operatorpkg instead of metav1.Condition for compatibility
	Conditions []status.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
type AzureFlexNodeClassList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AzureFlexNodeClass `json:"items"`
}
