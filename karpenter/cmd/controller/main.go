package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/karpenter-provider-azure/pkg/apis"
	"github.com/Azure/karpenter-provider-azure/pkg/cloudprovider"
	"github.com/Azure/karpenter-provider-azure/pkg/controllers"
	"github.com/Azure/karpenter-provider-azure/pkg/operator"
	"github.com/Azure/karpenter-provider-azure/pkg/operator/options"
	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/metrics"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/overlay"
	corecontrollers "sigs.k8s.io/karpenter/pkg/controllers"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	coreoperator "sigs.k8s.io/karpenter/pkg/operator"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/logging"
	coreoptions "sigs.k8s.io/karpenter/pkg/operator/options"

	kaitov1alpha1 "github.com/Azure/aks-flex/karpenter/pkg/apis/kaito/v1alpha1"
	"github.com/Azure/aks-flex/karpenter/pkg/apis/v1alpha1"
	flexcloudproviders "github.com/Azure/aks-flex/karpenter/pkg/cloudproviders"
	azureflex "github.com/Azure/aks-flex/karpenter/pkg/cloudproviders/azure"
	"github.com/Azure/aks-flex/karpenter/pkg/cloudproviders/kaito"
	"github.com/Azure/aks-flex/karpenter/pkg/cloudproviders/nebius"
	flexcontrollers "github.com/Azure/aks-flex/karpenter/pkg/controllers"
	flexoptions "github.com/Azure/aks-flex/karpenter/pkg/options"
	utilsk8s "github.com/Azure/aks-flex/karpenter/pkg/utils/k8s"
	wireguard "github.com/Azure/aks-flex/karpenter/pkg/utils/wireguard"
)

func init() {
	// FIXME: review this logic... are we sure this is the right way?
	v1alpha1.SchemeBuilder.AddToScheme(scheme.Scheme)
	kaitov1alpha1.SchemeBuilder.AddToScheme(scheme.Scheme)
}

func main() {
	ctx := injection.WithOptionsOrDie(context.Background(), coreoptions.Injectables...)
	logger := zapr.NewLogger(logging.NewLogger(ctx, "controller"))
	lo.Must0(
		operator.WaitForCRDs(ctx, 2*time.Minute, ctrl.GetConfigOrDie(), logger),
		"failed waiting for CRDs",
	)
	lo.Must0(
		waitForCRDs(
			ctx, 2*time.Minute, ctrl.GetConfigOrDie(), logger,
			&v1alpha1.NebiusNodeClass{},
			&v1alpha1.AzureFlexNodeClass{},
			&kaitov1alpha1.KaitoNodeClass{},
		),
		"failed waiting for flex CRDs",
	)

	ctx, op := operator.NewOperator(coreoperator.NewOperator())

	// TODO: Consider also dumping at least some core options
	logger.V(0).Info("Initial options", "options", options.FromContext(ctx).String())

	flexoptions.MustInitalizeStretchPlugin(ctx, op.GetConfig())

	hubCloudProvider := flexcloudproviders.NewCloudProvidersHub()
	defer func() {
		if err := hubCloudProvider.Close(ctx); err != nil {
			logger.Error(err, "closing cloud providers")
		}
	}()

	// AKS cloud provider...
	var aksCloudProvider *cloudprovider.CloudProvider
	{
		aksCloudProvider = cloudprovider.New(
			op.InstanceTypesProvider,
			op.VMInstanceProvider,
			op.AKSMachineProvider,
			op.EventRecorder,
			op.GetClient(),
			op.ImageProvider,
			op.InstanceTypeStore,
		)
		lo.Must0(op.AddHealthzCheck("cloud-provider", aksCloudProvider.LivenessProbe))
		hubCloudProvider.Register(aksCloudProvider, schema.GroupKind{
			Group: apis.Group,
			Kind:  "AKSNodeClass",
		}, "azure")
	}

	clusterVersion := lo.Must(utilsk8s.RetrieveClusterVersion(op.GetConfig()))
	clusterCA := lo.Must(utilsk8s.RetrieveClusterCA(op.GetConfig()))

	// nebius cloud provider...
	{
		wgAlloc := wireguard.NewIPAllocator(op.Manager.GetCache(), nebius.GroupKind, 30*time.Second)
		defer wgAlloc.Close()

		err := nebius.Register(
			ctx,
			hubCloudProvider,
			flexoptions.MustNewNebiusSDK(ctx),
			op.GetClient(),
			clusterVersion,
			clusterCA,
			wgAlloc,
		)
		lo.Must0(err, "registering nebius cloud provider")
	}

	// kaito
	{
		wgAlloc := wireguard.NewIPAllocator(op.Manager.GetCache(), kaito.GroupKind, 30*time.Second)
		defer wgAlloc.Close()

		err := kaito.Register(
			ctx,
			hubCloudProvider,
			clusterCA,
			wgAlloc,
		)
		lo.Must0(err, "registering kaito cloud provider")
	}

	// azure-flex (cross-region single-VM Azure cloud provider)
	{
		err := azureflex.Register(
			ctx,
			hubCloudProvider,
			op.GetClient(),
			clusterCA,
		)
		lo.Must0(err, "registering azure-flex cloud provider")
	}

	overlayUndecoratedCloudProvider := metrics.Decorate(hubCloudProvider)
	cloudProvider := overlay.Decorate(overlayUndecoratedCloudProvider, op.GetClient(), op.InstanceTypeStore)
	clusterState := state.NewCluster(op.Clock, op.GetClient(), cloudProvider)

	op.
		WithControllers(ctx, corecontrollers.NewControllers(
			ctx,
			op.Manager,
			op.Clock,
			op.GetClient(),
			op.EventRecorder,
			cloudProvider,
			overlayUndecoratedCloudProvider,
			clusterState,
			op.InstanceTypeStore,
		)...).
		WithControllers(ctx, controllers.NewControllers(
			ctx,
			op.Manager,
			op.GetClient(),
			op.EventRecorder,
			aksCloudProvider,
			op.VMInstanceProvider,
			op.AKSMachineProvider,
			// TODO: still need to refactor ImageProvider side of things.
			op.KubernetesVersionProvider,
			op.ImageProvider,
			op.InstanceTypesProvider,
			op.InClusterKubernetesInterface,
			op.AZClient.SubnetsClient(),
			op.AZClient.DiskEncryptionSetsClient(),
			options.FromContext(ctx).ParsedDiskEncryptionSetID,
		)...).
		WithControllers(ctx, flexcontrollers.NewControllers(
			ctx,
			op.GetClient(),
			op.EventRecorder,
		)...).
		Start(ctx)
}

func waitForCRDs(ctx context.Context, timeout time.Duration, config *rest.Config, logger logr.Logger, objs ...runtime.Object) error {
	client, err := rest.HTTPClientFor(config)
	if err != nil {
		return fmt.Errorf("creating kubernetes client: %w", err)
	}
	restMapper, err := apiutil.NewDynamicRESTMapper(config, client)
	if err != nil {
		return fmt.Errorf("creating dynamic rest mapper: %w", err)
	}

	requiredGVKs := make([]schema.GroupVersionKind, 0, len(objs))
	for _, obj := range objs {
		gvk, err := apiutil.GVKForObject(obj, scheme.Scheme)
		if err != nil {
			return fmt.Errorf("getting GVK for %T: %w", obj, err)
		}
		requiredGVKs = append(requiredGVKs, gvk)
	}

	logger.Info("waiting for flex CRDs to be available", "gvks", requiredGVKs, "timeout", timeout)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for _, gvk := range requiredGVKs {
		err := wait.PollUntilContextCancel(ctx, 10*time.Second, true, func(ctx context.Context) (bool, error) {
			if _, err := restMapper.RESTMapping(gvk.GroupKind(), gvk.Version); err != nil {
				if meta.IsNoMatchError(err) {
					logger.V(1).Info("waiting for flex CRD to be available", "gvk", gvk)
					return false, nil
				}
				return false, err
			}
			logger.V(1).Info("flex CRD is available", "gvk", gvk)
			return true, nil
		})
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				return fmt.Errorf("timed out waiting for CRD %s to be available", gvk)
			}
			return fmt.Errorf("failed to wait for CRD %s: %w", gvk, err)
		}
	}

	logger.Info("all flex CRDs are available")
	return nil
}
