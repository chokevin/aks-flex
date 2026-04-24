package azure

import (
	"testing"

	pluginapi "github.com/Azure/aks-flex/plugin/api"
	"github.com/Azure/aks-flex/plugin/pkg/services/agentpools/azure/flexvm"
	"github.com/Azure/aks-flex/plugin/pkg/services/agentpools/azure/ubuntu2404vmss"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func TestFlexAgentPoolFromGetResponse(t *testing.T) {
	t.Parallel()

	mkMeta := func(id string) *pluginapi.Metadata {
		return pluginapi.Metadata_builder{Id: proto.String(id)}.Build()
	}
	mkFlexResp := func(id string) *pluginapi.GetResponse {
		item, err := anypb.New(flexvm.AgentPool_builder{
			Metadata: mkMeta(id),
		}.Build())
		if err != nil {
			t.Fatalf("building flex anypb: %v", err)
		}
		return pluginapi.GetResponse_builder{Item: item}.Build()
	}
	mkVMSSResp := func(id string) *pluginapi.GetResponse {
		item, err := anypb.New(ubuntu2404vmss.AgentPool_builder{
			Metadata: mkMeta(id),
		}.Build())
		if err != nil {
			t.Fatalf("building vmss anypb: %v", err)
		}
		return pluginapi.GetResponse_builder{Item: item}.Build()
	}

	tests := []struct {
		name    string
		resp    *pluginapi.GetResponse
		wantID  string
		wantErr bool
	}{
		{
			name:    "nil response is not found",
			resp:    nil,
			wantErr: true,
		},
		{
			name:    "nil item is not found",
			resp:    pluginapi.GetResponse_builder{}.Build(),
			wantErr: true,
		},
		{
			name:    "wrong item type is not found",
			resp:    mkVMSSResp("node-1"),
			wantErr: true,
		},
		{
			name:   "flex agentpool item returns parsed object",
			resp:   mkFlexResp("node-2"),
			wantID: "node-2",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := flexAgentPoolFromGetResponse(tc.resp)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if !IsNotFound(err) {
					t.Fatalf("expected NotFound-style error, got: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.GetMetadata().GetId() != tc.wantID {
				t.Fatalf("got id %q, want %q", got.GetMetadata().GetId(), tc.wantID)
			}
		})
	}
}
