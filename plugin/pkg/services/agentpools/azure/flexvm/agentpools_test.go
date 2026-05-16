package flexvm

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/Azure/aks-flex/plugin/api"
	"github.com/Azure/aks-flex/plugin/pkg/db"
	"github.com/Azure/aks-flex/plugin/pkg/helper"
)

func tempDB(t *testing.T) *db.StupidDB {
	t.Helper()
	store := db.NewStupidDB(filepath.Join(t.TempDir(), "agentpools.db"))
	t.Cleanup(store.Close)
	return store
}

func testAgentPool(id string, status *AgentPoolStatus) *AgentPool {
	return AgentPool_builder{
		Metadata: api.Metadata_builder{
			Id: proto.String(id),
		}.Build(),
		Status: status,
	}.Build()
}

func TestPersistIncompleteAgentPoolStoresFailedCreate(t *testing.T) {
	store := tempDB(t)
	ap := testAgentPool("nodeclaim-1", nil)

	persistIncompleteAgentPool(store, ap)

	got, ok := store.Get("nodeclaim-1")
	require.True(t, ok)
	require.Equal(t, "nodeclaim-1", got.GetMetadata().GetId())
	gotAP, err := helper.To[*AgentPool](got)
	require.NoError(t, err)
	require.NotNil(t, gotAP.GetStatus().GetCreatedAt())
}

func TestPersistIncompleteAgentPoolDoesNotOverwriteCompletedStatus(t *testing.T) {
	store := tempDB(t)
	store.CreateOrUpdate(testAgentPool("nodeclaim-1", AgentPoolStatus_builder{
		VmResourceId: proto.String("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/nodeclaim-1"),
	}.Build()))

	persistIncompleteAgentPool(store, testAgentPool("nodeclaim-1", nil))

	gotObj, ok := store.Get("nodeclaim-1")
	require.True(t, ok)
	got, err := helper.To[*AgentPool](gotObj)
	require.NoError(t, err)
	require.NotEmpty(t, got.GetStatus().GetVmResourceId())
}

func TestPersistIncompleteAgentPoolPreservesPendingCreatedAt(t *testing.T) {
	store := tempDB(t)
	createdAt := timestamppb.New(time.Now().Add(-time.Hour))
	store.CreateOrUpdate(testAgentPool("nodeclaim-1", AgentPoolStatus_builder{
		CreatedAt: createdAt,
	}.Build()))

	persistIncompleteAgentPool(store, testAgentPool("nodeclaim-1", nil))

	gotObj, ok := store.Get("nodeclaim-1")
	require.True(t, ok)
	got, err := helper.To[*AgentPool](gotObj)
	require.NoError(t, err)
	require.Equal(t, createdAt.AsTime(), got.GetStatus().GetCreatedAt().AsTime())
}

func TestDeleteIncompleteAgentPoolOnlyDeletesPendingRecords(t *testing.T) {
	store := tempDB(t)
	store.CreateOrUpdate(testAgentPool("pending", nil))
	store.CreateOrUpdate(testAgentPool("complete", AgentPoolStatus_builder{
		VmResourceId: proto.String("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/complete"),
	}.Build()))

	deleteIncompleteAgentPool(store, "pending")
	deleteIncompleteAgentPool(store, "complete")

	_, ok := store.Get("pending")
	require.False(t, ok)
	_, ok = store.Get("complete")
	require.True(t, ok)
}
