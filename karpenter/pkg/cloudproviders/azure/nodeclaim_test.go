package azure

import (
	"context"
	"strings"
	"testing"
	"time"

	karpoptions "github.com/Azure/karpenter-provider-azure/pkg/operator/options"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	corecloudprovider "sigs.k8s.io/karpenter/pkg/cloudprovider"

	"github.com/Azure/aks-flex/karpenter/pkg/apis/v1alpha1"
	stretchapi "github.com/Azure/aks-flex/plugin/api"
	agentpoolsapi "github.com/Azure/aks-flex/plugin/pkg/services/agentpools/api"
	flexvm "github.com/Azure/aks-flex/plugin/pkg/services/agentpools/azure/flexvm"
	nebiusinstance "github.com/Azure/aks-flex/plugin/pkg/services/agentpools/nebius/instance"
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

func TestDeleteCleansAgentPoolWithoutProviderID(t *testing.T) {
	fake := newFakeAgentPoolsClient(testFlexVMAgentPool("nodeclaim-1", "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/nodeclaim-1"))
	cp := &CloudProvider{stretchAgentPoolsClient: fake}

	err := cp.Delete(context.Background(), &v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "nodeclaim-1"},
	})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if got := <-fake.deleted; got != "nodeclaim-1" {
		t.Fatalf("expected delete for nodeclaim-1, got %q", got)
	}
}

func TestListSkipsAndCleansIncompleteAgentPools(t *testing.T) {
	fake := newFakeAgentPoolsClient(testIncompleteFlexVMAgentPool("nodeclaim-1", time.Now().Add(-incompleteAgentPoolCleanupDelay-time.Minute)))
	cp := &CloudProvider{stretchAgentPoolsClient: fake}

	nodeClaims, err := cp.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(nodeClaims) != 0 {
		t.Fatalf("expected incomplete agent pool to be skipped, got %d nodeclaims", len(nodeClaims))
	}

	select {
	case got := <-fake.deleted:
		if got != "nodeclaim-1" {
			t.Fatalf("expected cleanup delete for nodeclaim-1, got %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for incomplete agent pool cleanup")
	}
}

func TestListDefersFreshIncompleteAgentPoolCleanup(t *testing.T) {
	fake := newFakeAgentPoolsClient(testIncompleteFlexVMAgentPool("nodeclaim-1", time.Now()))
	cp := &CloudProvider{stretchAgentPoolsClient: fake}

	nodeClaims, err := cp.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(nodeClaims) != 0 {
		t.Fatalf("expected incomplete agent pool to be skipped, got %d nodeclaims", len(nodeClaims))
	}

	select {
	case got := <-fake.deleted:
		t.Fatalf("fresh incomplete agent pool should not be cleaned up yet, deleted %q", got)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestListIgnoresNonAzureFlexAgentPools(t *testing.T) {
	fake := newFakeAgentPoolsClient(testFlexVMAgentPool("nodeclaim-1", "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/nodeclaim-1"))
	other, err := anypb.New(nebiusinstance.AgentPool_builder{}.Build())
	if err != nil {
		t.Fatalf("building nebius agent pool Any: %v", err)
	}
	fake.rawItems = append(fake.rawItems, other)
	cp := &CloudProvider{stretchAgentPoolsClient: fake}

	nodeClaims, err := cp.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(nodeClaims) != 1 {
		t.Fatalf("expected only the azure-flex agent pool, got %d nodeclaims", len(nodeClaims))
	}
	if nodeClaims[0].Name != "nodeclaim-1" {
		t.Fatalf("expected nodeclaim-1, got %q", nodeClaims[0].Name)
	}
}

func TestNodeClaimToAgentPoolPropagatesH200LabelsAndTaints(t *testing.T) {
	osDiskSize := int32(256)
	nodeClass := &v1alpha1.AzureFlexNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "h200-eastus2"},
		Spec: v1alpha1.AzureFlexNodeClassSpec{
			SubscriptionID: "sub",
			Location:       "eastus2",
			ResourceGroup:  "rg",
			SubnetID:       "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Network/virtualNetworks/vnet/subnets/nodes",
			OSDiskSizeGB:   &osDiskSize,
		},
	}
	nodeClaim := &v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "flex-h200-abcde",
			Labels: map[string]string{
				"gpu":                    "h200",
				"nvidia.com/gpu.present": "true",
				"rune.ai/gpu-family":     "h200",
			},
		},
		Spec: v1.NodeClaimSpec{
			Taints: []corev1.Taint{
				{Key: "nvidia.com/gpu", Value: "present", Effect: corev1.TaintEffectNoSchedule},
			},
			StartupTaints: []corev1.Taint{
				{Key: "nvidia.com/gpu.init", Value: "true", Effect: corev1.TaintEffectNoSchedule},
			},
		},
	}

	ap := nodeClaimToAgentPool(
		&karpoptions.Options{
			ClusterEndpoint:                "https://cluster.example:443",
			KubeletClientTLSBootstrapToken: "token",
			NodeResourceGroup:              "MC_rg_cluster_eastus2",
		},
		[]byte("ca"),
		nodeClass,
		nodeClaim,
		&corecloudprovider.InstanceType{Name: "Standard_ND96isr_H200_v5"},
	)

	labels := ap.GetSpec().GetKubeadm().GetNodeLabels()
	if labels["gpu"] != "h200" {
		t.Fatalf("expected gpu=h200 label, got %q", labels["gpu"])
	}
	if labels["nvidia.com/gpu.present"] != "true" {
		t.Fatalf("expected nvidia.com/gpu.present=true label, got %q", labels["nvidia.com/gpu.present"])
	}
	if labels["node.kubernetes.io/instance-type"] != "Standard_ND96isr_H200_v5" {
		t.Fatalf("expected stable instance-type label, got %q", labels["node.kubernetes.io/instance-type"])
	}

	taints := ap.GetSpec().GetKubeadm().GetK8SRegisterTaints()
	assertHasTaint(t, taints, corev1.Taint{Key: "nvidia.com/gpu", Value: "present", Effect: corev1.TaintEffectNoSchedule})
	assertHasTaint(t, taints, corev1.Taint{Key: "nvidia.com/gpu.init", Value: "true", Effect: corev1.TaintEffectNoSchedule})
	assertHasTaint(t, taints, v1.UnregisteredNoExecuteTaint)

	ap.SetStatus(flexvm.AgentPoolStatus_builder{
		VmResourceId: proto.String("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/flex-h200-abcde"),
		CreatedAt:    timestamppb.Now(),
	}.Build())
	listed := agentPoolToNodeClaim(ap, nil)
	if listed.Labels["gpu"] != "h200" {
		t.Fatalf("expected listed nodeclaim to retain gpu=h200 label, got %q", listed.Labels["gpu"])
	}
	if listed.Labels["rune.ai/gpu-family"] != "h200" {
		t.Fatalf("expected listed nodeclaim to retain rune.ai/gpu-family=h200 label, got %q", listed.Labels["rune.ai/gpu-family"])
	}
}

func assertHasTaint(t *testing.T, taints []corev1.Taint, want corev1.Taint) {
	t.Helper()
	for i := range taints {
		if want.MatchTaint(&taints[i]) {
			return
		}
	}
	t.Fatalf("expected taint %+v in %+v", want, taints)
}

type fakeAgentPoolsClient struct {
	items    map[string]*flexvm.AgentPool
	rawItems []*anypb.Any
	deleted  chan string
}

func newFakeAgentPoolsClient(items ...*flexvm.AgentPool) *fakeAgentPoolsClient {
	f := &fakeAgentPoolsClient{
		items:   map[string]*flexvm.AgentPool{},
		deleted: make(chan string, len(items)+1),
	}
	for _, item := range items {
		f.items[item.GetMetadata().GetId()] = item
	}
	return f
}

var _ agentpoolsapi.AgentPoolsClient = (*fakeAgentPoolsClient)(nil)

func (f *fakeAgentPoolsClient) CreateOrUpdate(context.Context, *stretchapi.CreateOrUpdateRequest, ...grpc.CallOption) (*stretchapi.CreateOrUpdateResponse, error) {
	return nil, status.Error(codes.Unimplemented, "not implemented")
}

func (f *fakeAgentPoolsClient) List(context.Context, *stretchapi.ListRequest, ...grpc.CallOption) (*stretchapi.ListResponse, error) {
	items := make([]*anypb.Any, 0, len(f.items)+len(f.rawItems))
	for _, item := range f.items {
		anyItem, err := anypb.New(item)
		if err != nil {
			return nil, err
		}
		items = append(items, anyItem)
	}
	items = append(items, f.rawItems...)
	return stretchapi.ListResponse_builder{Items: items}.Build(), nil
}

func (f *fakeAgentPoolsClient) Get(_ context.Context, req *stretchapi.GetRequest, _ ...grpc.CallOption) (*stretchapi.GetResponse, error) {
	item, ok := f.items[req.GetId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "")
	}
	anyItem, err := anypb.New(item)
	if err != nil {
		return nil, err
	}
	return stretchapi.GetResponse_builder{Item: anyItem}.Build(), nil
}

func (f *fakeAgentPoolsClient) Delete(_ context.Context, req *stretchapi.DeleteRequest, _ ...grpc.CallOption) (*stretchapi.DeleteResponse, error) {
	delete(f.items, req.GetId())
	f.deleted <- req.GetId()
	return stretchapi.DeleteResponse_builder{}.Build(), nil
}

func testFlexVMAgentPool(id, vmResourceID string) *flexvm.AgentPool {
	return flexvm.AgentPool_builder{
		Metadata: stretchapi.Metadata_builder{
			Id: proto.String(id),
		}.Build(),
		Status: flexvm.AgentPoolStatus_builder{
			VmResourceId: proto.String(vmResourceID),
		}.Build(),
	}.Build()
}

func testIncompleteFlexVMAgentPool(id string, createdAt time.Time) *flexvm.AgentPool {
	return flexvm.AgentPool_builder{
		Metadata: stretchapi.Metadata_builder{
			Id: proto.String(id),
		}.Build(),
		Status: flexvm.AgentPoolStatus_builder{
			CreatedAt: timestamppb.New(createdAt),
		}.Build(),
	}.Build()
}
