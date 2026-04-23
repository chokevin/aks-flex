package azure

import (
	"context"
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	opcontroller "github.com/awslabs/operatorpkg/controller"
	"github.com/awslabs/operatorpkg/reasonable"
	"k8s.io/apimachinery/pkg/api/equality"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/karpenter/pkg/operator/injection"

	"github.com/Azure/aks-flex/karpenter/pkg/apis/v1alpha1"
)

const controllerNameStatus = "azureflex_nodeclass.status"

type NodeClassStatusController struct {
	kubeClient client.Client
}

var (
	_ opcontroller.Controller                                  = (*NodeClassStatusController)(nil)
	_ reconcile.ObjectReconciler[*v1alpha1.AzureFlexNodeClass] = (*NodeClassStatusController)(nil)
)

func NewNodeClassStatusController(kubeClient client.Client) *NodeClassStatusController {
	return &NodeClassStatusController{kubeClient: kubeClient}
}

func (c *NodeClassStatusController) Register(_ context.Context, mgr manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(mgr).
		Named(controllerNameStatus).
		For(&v1alpha1.AzureFlexNodeClass{}).
		WithOptions(controller.Options{
			RateLimiter:             reasonable.RateLimiter(),
			MaxConcurrentReconciles: 10,
		}).
		Complete(reconcile.AsReconciler(mgr.GetClient(), c))
}

func (c *NodeClassStatusController) Reconcile(
	ctx context.Context,
	nodeClass *v1alpha1.AzureFlexNodeClass,
) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, controllerNameStatus)

	existing := nodeClass
	future := nodeClass.DeepCopy()

	if err := c.ensureFinalizer(ctx, future); err != nil {
		return reconcile.Result{}, err
	}

	if err := validateSpec(future.Spec); err != nil {
		future.StatusConditions().SetFalse(
			v1alpha1.ConditionTypeValidationSucceeded, "InvalidSpec", err.Error(),
		)
	} else {
		future.StatusConditions().SetTrue(v1alpha1.ConditionTypeValidationSucceeded)
	}

	if !equality.Semantic.DeepEqual(existing, future) {
		if err := c.kubeClient.Status().Patch(ctx, future, client.MergeFrom(existing)); err != nil {
			return reconcile.Result{}, err
		}
	}

	return reconcile.Result{}, nil
}

func (c *NodeClassStatusController) ensureFinalizer(
	ctx context.Context,
	nodeClass *v1alpha1.AzureFlexNodeClass,
) error {
	if controllerutil.ContainsFinalizer(nodeClass, v1alpha1.TerminationFinalizer) {
		return nil
	}
	base := nodeClass.DeepCopy()
	controllerutil.AddFinalizer(nodeClass, v1alpha1.TerminationFinalizer)
	if err := c.kubeClient.Patch(ctx, nodeClass, client.MergeFrom(base)); err != nil {
		return fmt.Errorf("patch finalizer: %w", err)
	}
	return nil
}

// validateSpec performs cheap shape checks. Anything Azure-side (subnet
// existence, RG existence, identity perms) is detected on Create.
func validateSpec(spec v1alpha1.AzureFlexNodeClassSpec) error {
	if strings.TrimSpace(spec.SubscriptionID) == "" {
		return fmt.Errorf("subscriptionID is required")
	}
	if strings.TrimSpace(spec.Location) == "" {
		return fmt.Errorf("location is required")
	}
	if strings.TrimSpace(spec.ResourceGroup) == "" {
		return fmt.Errorf("resourceGroup is required")
	}
	if !strings.HasPrefix(spec.SubnetID, "/subscriptions/") {
		return fmt.Errorf("subnetID %q must be a full ARM resource ID", spec.SubnetID)
	}
	if _, err := arm.ParseResourceID(spec.SubnetID); err != nil {
		return fmt.Errorf("subnetID %q is not a valid ARM resource ID: %w", spec.SubnetID, err)
	}
	if spec.ImageReference != nil && spec.ImageID != nil && *spec.ImageID != "" {
		return fmt.Errorf("imageReference and imageID are mutually exclusive")
	}
	return nil
}
