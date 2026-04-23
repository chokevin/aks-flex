// Package flexvm implements the cross-region Azure VM agent pool service.
//
// This service creates a single Microsoft.Compute/virtualMachines (and a
// dedicated NIC + OS disk) per AgentPool. It is intentionally separate from
// ubuntu2404vmss (which uses a Virtual Machine Scale Set) because per-VM
// management is required for Karpenter's per-NodeClaim lifecycle and for
// straightforward cross-region placement.
//
// Authentication: the plugin process is expected to authenticate via
// DefaultAzureCredential (e.g. a workload identity / managed identity) and
// hold Contributor on the target subscription / resource group / subnet.
//
// Resource naming is fully deterministic from the AgentPool ID (which is the
// NodeClaim name) so retries are idempotent:
//   - VM name  = <agentpool-id>
//   - NIC name = <agentpool-id>-nic
//   - IP cfg name = ipconfig
//
// The NIC and OS disk are configured with DeleteOption=Delete so a single VM
// delete cascades cleanup; this is critical for idempotency on Karpenter
// retries.
package flexvm

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/network/armnetwork/v8"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Azure/aks-flex/plugin/api"
	"github.com/Azure/aks-flex/plugin/pkg/db"
	"github.com/Azure/aks-flex/plugin/pkg/helper"
	agentpools "github.com/Azure/aks-flex/plugin/pkg/services/agentpools/api"
	"github.com/Azure/aks-flex/plugin/pkg/services/agentpools/userdata/flex"
	"github.com/Azure/aks-flex/plugin/pkg/topology"
)

var _ api.Object = (*AgentPool)(nil)

// Default DSVM image. SecurityType MUST stay "Standard" — TrustedLaunch is
// known to break the DSVM image (verified during manual H200 bringup).
const (
	defaultImagePublisher = "microsoft-dsvm"
	defaultImageOffer     = "ubuntu-hpc"
	defaultImageSKU       = "2204"
	defaultImageVersion   = "latest"

	defaultAdminUsername = "ubuntu"
)

type agentpoolsServer struct {
	agentpools.UnimplementedAgentPoolsServer
	storage db.RODB

	credentials azcore.TokenCredential
}

func NewAgentPoolsServer(storage db.RODB) (agentpools.AgentPoolsServer, error) {
	credentials, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	return &agentpoolsServer{
		storage:     storage,
		credentials: credentials,
	}, nil
}

func (srv *agentpoolsServer) CreateOrUpdate(ctx context.Context, req *api.CreateOrUpdateRequest) (resp *api.CreateOrUpdateResponse, err error) {
	ap, err := helper.AnyTo[*AgentPool](req.GetItem())
	if err != nil {
		return nil, err
	}
	if err := validateSpec(ap.GetSpec()); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	spec := ap.GetSpec()
	vmName := ap.GetMetadata().GetId()
	nicName := vmName + "-nic"

	// Annotate kubeadm node labels with cross-region topology hints. Note:
	// region is the *target* region (the VM's region), which may differ
	// from the AKS control-plane region — that's the whole point of flexvm.
	kubeadmConfig := spec.GetKubeadm()
	kubeadmConfig.AddNodeLabels(map[string]string{
		topology.NodeLabelKeyCloud:        "azure",
		topology.NodeLabelKeyRegion:       strings.ToLower(spec.GetLocation()),
		topology.NodeLabelKeyInstanceType: strings.ToLower(spec.GetVmSize()),
	})

	userData, err := flex.UserData(
		flex.WithKubeadmConfig(kubeadmConfig),
	)
	if err != nil {
		return nil, fmt.Errorf("rendering flex user data: %w", err)
	}
	userDataBytes, err := userData.Gzip()
	if err != nil {
		return nil, fmt.Errorf("gzipping user data: %w", err)
	}
	userDataB64 := base64.StdEncoding.EncodeToString(userDataBytes)

	// 1. NIC (idempotent: same name → ARM updates in place).
	nicsClient, err := armnetwork.NewInterfacesClient(spec.GetSubscriptionId(), srv.credentials, nil)
	if err != nil {
		return nil, fmt.Errorf("creating NIC client: %w", err)
	}
	nicParams := armnetwork.Interface{
		Location: to.Ptr(spec.GetLocation()),
		Tags:     toARMTags(spec.GetTags()),
		Properties: &armnetwork.InterfacePropertiesFormat{
			IPConfigurations: []*armnetwork.InterfaceIPConfiguration{
				{
					Name: to.Ptr("ipconfig"),
					Properties: &armnetwork.InterfaceIPConfigurationPropertiesFormat{
						Subnet:                    &armnetwork.Subnet{ID: to.Ptr(spec.GetSubnetId())},
						PrivateIPAllocationMethod: to.Ptr(armnetwork.IPAllocationMethodDynamic),
					},
				},
			},
		},
	}
	if spec.GetAllocatePublicIp() {
		// validateSpec rejects this; the branch is kept as a compile-time
		// reminder for when Phase 2 adds PIP support.
		return nil, errors.New("allocate_public_ip=true is not supported in Phase 1")
	}
	nicPoller, err := nicsClient.BeginCreateOrUpdate(ctx, spec.GetResourceGroup(), nicName, nicParams, nil)
	if err != nil {
		return nil, fmt.Errorf("creating NIC %q: %w", nicName, err)
	}
	nicResp, err := nicPoller.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("polling NIC creation %q: %w", nicName, err)
	}
	nicID := *nicResp.ID

	// Best-effort NIC cleanup if anything between here and the successful VM
	// creation fails. Without this, every quota-rejected / ARM-rejected VM
	// create on a Karpenter retry loop leaks one NIC, which exhausts the
	// subnet (observed: ~250 orphan NICs accumulating per hour during
	// quota-blocked H100 churn). Uses a fresh background context because the
	// gRPC ctx is often already cancelled by the time we land here on retry.
	// Best-effort NIC cleanup if anything between here and the successful VM
	// creation fails.
	//
	// Azure platform quirk: after *any* VM CreateOrUpdate attempt (even one
	// that fails synchronously with 409 quota), ARM reserves the referenced
	// NIC for the target VM name for 180 seconds. Delete attempts during
	// that window return 400 NicReservedForAnotherVm. We therefore cannot
	// clean up synchronously inside the gRPC handler — Karpenter expects a
	// fast error response so it can back off and retry. We spawn a detached
	// goroutine that waits out the reservation and retries with backoff.
	//
	// Best-effort contract:
	//   - Cleanup survives only while the plugin process is alive. On pod
	//     restart, any in-flight orphan NICs need manual sweep or a periodic
	//     reconciler (future work).
	//   - Under sustained quota exhaustion, the number of sleeping
	//     cleanup goroutines is bounded by the retry rate (observed ~7/min)
	//     times the cleanup window (~4 min) — a few dozen max.
	nicCleanedUp := false
	defer func() {
		if err == nil || nicCleanedUp {
			return
		}
		go cleanupReservedNIC(nicsClient, spec.GetResourceGroup(), nicName)
	}()

	// 2. VM. NIC + OS disk both set DeleteOption=Delete so a single VM
	// delete cascades — this is critical for Karpenter retry idempotency.
	vmsClient, err := armcompute.NewVirtualMachinesClient(spec.GetSubscriptionId(), srv.credentials, nil)
	if err != nil {
		return nil, fmt.Errorf("creating VM client: %w", err)
	}
	imageRef, err := buildImageReference(spec)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	osDiskSizeGB := spec.GetOsDiskSizeGb()
	if osDiskSizeGB == 0 {
		osDiskSizeGB = 128
	}

	vmParams := armcompute.VirtualMachine{
		Location: to.Ptr(spec.GetLocation()),
		Tags:     toARMTags(spec.GetTags()),
		Properties: &armcompute.VirtualMachineProperties{
			HardwareProfile: &armcompute.HardwareProfile{
				VMSize: to.Ptr(armcompute.VirtualMachineSizeTypes(spec.GetVmSize())),
			},
			SecurityProfile: &armcompute.SecurityProfile{
				// Standard only — TrustedLaunch is deferred (breaks DSVM).
				SecurityType: nil,
			},
			NetworkProfile: &armcompute.NetworkProfile{
				NetworkInterfaces: []*armcompute.NetworkInterfaceReference{
					{
						ID: to.Ptr(nicID),
						Properties: &armcompute.NetworkInterfaceReferenceProperties{
							Primary:      to.Ptr(true),
							DeleteOption: to.Ptr(armcompute.DeleteOptionsDelete),
						},
					},
				},
			},
			OSProfile: &armcompute.OSProfile{
				ComputerName:  to.Ptr(vmName),
				AdminUsername: to.Ptr(defaultAdminUsername),
				LinuxConfiguration: &armcompute.LinuxConfiguration{
					DisablePasswordAuthentication: to.Ptr(true),
					SSH:                           buildSSHConfig(spec.GetSshPublicKeys()),
				},
			},
			StorageProfile: &armcompute.StorageProfile{
				ImageReference: imageRef,
				OSDisk: &armcompute.OSDisk{
					CreateOption: to.Ptr(armcompute.DiskCreateOptionTypesFromImage),
					Caching:      to.Ptr(armcompute.CachingTypesReadWrite),
					DiskSizeGB:   to.Ptr(osDiskSizeGB),
					DeleteOption: to.Ptr(armcompute.DiskDeleteOptionTypesDelete),
					ManagedDisk: &armcompute.ManagedDiskParameters{
						StorageAccountType: to.Ptr(armcompute.StorageAccountTypesPremiumLRS),
					},
				},
			},
			// UserData (NOT customData): the bootstrap renderer expects to read
			// from the IMDS userData endpoint. Mirrors the ubuntu2404vmss path.
			UserData: to.Ptr(userDataB64),
		},
	}

	vmPoller, err := vmsClient.BeginCreateOrUpdate(ctx, spec.GetResourceGroup(), vmName, vmParams, nil)
	if err != nil {
		return nil, fmt.Errorf("creating VM %q: %w", vmName, err)
	}
	vmResp, err := vmPoller.PollUntilDone(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("polling VM creation %q: %w", vmName, err)
	}
	if vmResp.ID == nil {
		return nil, fmt.Errorf("VM %q created but Azure returned nil resource ID", vmName)
	}
	// VM is up and owns the NIC via DeleteOption=Delete; suppress the deferred
	// NIC cleanup so a downstream marshal failure doesn't tear down the node.
	nicCleanedUp = true

	ap.SetStatus(AgentPoolStatus_builder{
		VmResourceId: vmResp.ID,
		CreatedAt:    timestamppb.Now(),
	}.Build())

	item, err := anypb.New(ap)
	if err != nil {
		return nil, err
	}

	return api.CreateOrUpdateResponse_builder{
		Item: item,
	}.Build(), nil
}

func (srv *agentpoolsServer) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	obj, ok := srv.storage.Get(req.GetId())
	if !ok {
		return api.DeleteResponse_builder{}.Build(), nil
	}

	ap, err := helper.To[*AgentPool](obj)
	if err != nil {
		return nil, err
	}
	spec := ap.GetSpec()

	vmName := ap.GetMetadata().GetId()
	nicName := vmName + "-nic"

	// Delete VM first; NIC + OS disk cascade because we set DeleteOption=Delete on create.
	vmsClient, err := armcompute.NewVirtualMachinesClient(spec.GetSubscriptionId(), srv.credentials, nil)
	if err != nil {
		return nil, fmt.Errorf("creating VM client: %w", err)
	}
	vmPoller, err := vmsClient.BeginDelete(ctx, spec.GetResourceGroup(), vmName, &armcompute.VirtualMachinesClientBeginDeleteOptions{
		ForceDeletion: to.Ptr(true),
	})
	if err != nil && !isNotFound(err) {
		return nil, fmt.Errorf("starting VM delete %q: %w", vmName, err)
	}
	if vmPoller != nil {
		if _, err := vmPoller.PollUntilDone(ctx, nil); err != nil && !isNotFound(err) {
			return nil, fmt.Errorf("polling VM delete %q: %w", vmName, err)
		}
	}

	// Best-effort NIC delete in case the VM never made it to a state where
	// DeleteOption applied (e.g. failed mid-create). Idempotent. Uses a fresh
	// context so cleanup still runs if the caller's ctx was cancelled mid-Delete.
	nicCtx, nicCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer nicCancel()
	nicsClient, err := armnetwork.NewInterfacesClient(spec.GetSubscriptionId(), srv.credentials, nil)
	if err != nil {
		return nil, fmt.Errorf("creating NIC client: %w", err)
	}
	nicPoller, err := nicsClient.BeginDelete(nicCtx, spec.GetResourceGroup(), nicName, nil)
	if err != nil && !isNotFound(err) {
		return nil, fmt.Errorf("starting NIC delete %q: %w", nicName, err)
	}
	if nicPoller != nil {
		if _, err := nicPoller.PollUntilDone(nicCtx, nil); err != nil && !isNotFound(err) {
			return nil, fmt.Errorf("polling NIC delete %q: %w", nicName, err)
		}
	}

	return api.DeleteResponse_builder{}.Build(), nil
}

// validateSpec fails fast with the cheap structural checks that don't need an Azure round-trip.
func validateSpec(spec *AgentPoolSpec) error {
	if spec.GetSubscriptionId() == "" {
		return errors.New("subscription_id is required")
	}
	if spec.GetResourceGroup() == "" {
		return errors.New("resource_group is required")
	}
	if spec.GetLocation() == "" {
		return errors.New("location is required")
	}
	if spec.GetSubnetId() == "" {
		return errors.New("subnet_id is required")
	}
	if _, err := arm.ParseResourceID(spec.GetSubnetId()); err != nil {
		return fmt.Errorf("subnet_id %q is not a valid ARM resource id: %w", spec.GetSubnetId(), err)
	}
	if spec.GetVmSize() == "" {
		return errors.New("vm_size is required")
	}
	if spec.GetImageId() != "" && spec.GetImageReference() != nil {
		return errors.New("image_id and image_reference are mutually exclusive")
	}
	if st := spec.GetSecurityType(); st != "" && st != "Standard" {
		return fmt.Errorf("unsupported security_type %q (only Standard is supported in Phase 1)", st)
	}
	// Public IP per NIC is not implemented in Phase 1. Reject instead of
	// silently creating a private-only NIC when callers expect a public one.
	if spec.GetAllocatePublicIp() {
		return errors.New("allocate_public_ip=true is not supported in Phase 1")
	}
	// kubeadm config carries the AKS bootstrap token + CA and is used to
	// render userdata; a nil value here would panic in CreateOrUpdate.
	if spec.GetKubeadm() == nil {
		return errors.New("kubeadm is required")
	}
	return nil
}

func buildImageReference(spec *AgentPoolSpec) (*armcompute.ImageReference, error) {
	if spec.GetImageId() != "" {
		return &armcompute.ImageReference{
			ID: to.Ptr(spec.GetImageId()),
		}, nil
	}
	ref := spec.GetImageReference()
	if ref == nil {
		return &armcompute.ImageReference{
			Publisher: to.Ptr(defaultImagePublisher),
			Offer:     to.Ptr(defaultImageOffer),
			SKU:       to.Ptr(defaultImageSKU),
			Version:   to.Ptr(defaultImageVersion),
		}, nil
	}
	if ref.GetPublisher() == "" || ref.GetOffer() == "" || ref.GetSku() == "" {
		return nil, errors.New("image_reference requires publisher, offer, and sku")
	}
	version := ref.GetVersion()
	if version == "" {
		version = defaultImageVersion
	}
	return &armcompute.ImageReference{
		Publisher: to.Ptr(ref.GetPublisher()),
		Offer:     to.Ptr(ref.GetOffer()),
		SKU:       to.Ptr(ref.GetSku()),
		Version:   to.Ptr(version),
	}, nil
}

func buildSSHConfig(keys []string) *armcompute.SSHConfiguration {
	if len(keys) == 0 {
		return nil
	}
	pks := make([]*armcompute.SSHPublicKey, 0, len(keys))
	for _, k := range keys {
		pks = append(pks, &armcompute.SSHPublicKey{
			Path:    to.Ptr("/home/" + defaultAdminUsername + "/.ssh/authorized_keys"),
			KeyData: to.Ptr(k),
		})
	}
	return &armcompute.SSHConfiguration{PublicKeys: pks}
}

func toARMTags(tags map[string]string) map[string]*string {
	if len(tags) == 0 {
		return nil
	}
	out := make(map[string]*string, len(tags))
	for k, v := range tags {
		out[k] = to.Ptr(v)
	}
	return out
}

func isNotFound(err error) bool {
	var rerr *azcore.ResponseError
	if errors.As(err, &rerr) {
		return rerr.StatusCode == 404
	}
	return false
}

// cleanupReservedNIC deletes an orphan NIC after the 180s ARM reservation
// window expires. Runs detached (its own goroutine); intended only for the
// post-VM-create-failure path where the NIC is guaranteed to outlive its
// caller's request context. All errors are best-effort logged; under
// sustained ARM turbulence this may leave orphans that a human or periodic
// reconciler will need to sweep.
func cleanupReservedNIC(nicsClient *armnetwork.InterfacesClient, resourceGroup, nicName string) {
	// Wait out the ARM 180s NIC reservation window, plus slack for clock
	// skew and any in-flight VM-create retry that might re-reserve the NIC
	// on the same name (Karpenter retries DO use new nodeclaim names, so
	// this is belt-and-suspenders).
	time.Sleep(3*time.Minute + 30*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	for attempt := 0; attempt < 5; attempt++ {
		delPoller, delErr := nicsClient.BeginDelete(ctx, resourceGroup, nicName, nil)
		if delErr == nil {
			if _, pollErr := delPoller.PollUntilDone(ctx, nil); pollErr == nil {
				slog.Info("flexvm orphan NIC cleanup succeeded",
					"nic", nicName, "attempt", attempt)
				return
			} else {
				slog.Warn("flexvm orphan NIC poll failed",
					"nic", nicName, "attempt", attempt, "err", pollErr)
			}
		} else {
			// 404 = already gone (raced with someone else). Treat as success.
			if isNotFound(delErr) {
				slog.Info("flexvm orphan NIC already gone",
					"nic", nicName, "attempt", attempt)
				return
			}
			slog.Warn("flexvm orphan NIC BeginDelete failed",
				"nic", nicName, "attempt", attempt, "err", delErr)
		}

		select {
		case <-time.After(30 * time.Second):
		case <-ctx.Done():
			slog.Error("flexvm orphan NIC cleanup timed out",
				"nic", nicName, "attempts", attempt+1)
			return
		}
	}
	slog.Error("flexvm orphan NIC cleanup exhausted retries", "nic", nicName)
}
