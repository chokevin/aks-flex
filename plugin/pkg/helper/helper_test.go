package helper_test

import (
	"context"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	stretchapi "github.com/Azure/aks-flex/plugin/api"
	"github.com/Azure/aks-flex/plugin/pkg/helper"
	flexvm "github.com/Azure/aks-flex/plugin/pkg/services/agentpools/azure/flexvm"
	nebiusinstance "github.com/Azure/aks-flex/plugin/pkg/services/agentpools/nebius/instance"
)

func TestListByTypeFiltersDifferentAnyTypes(t *testing.T) {
	flexAny := mustAny(t, flexvm.AgentPool_builder{
		Metadata: stretchapi.Metadata_builder{Id: proto.String("flex")}.Build(),
	}.Build())
	nebiusAny := mustAny(t, nebiusinstance.AgentPool_builder{
		Metadata: stretchapi.Metadata_builder{Id: proto.String("nebius")}.Build(),
	}.Build())

	var gotID string
	list := func(_ context.Context, req *stretchapi.ListRequest, _ ...grpc.CallOption) (*stretchapi.ListResponse, error) {
		gotID = req.GetId()
		return stretchapi.ListResponse_builder{Items: []*anypb.Any{flexAny, nebiusAny}}.Build(), nil
	}

	got, err := helper.ListByType[*flexvm.AgentPool](list, context.Background(), "nodeclaims")
	if err != nil {
		t.Fatalf("ListByType: %v", err)
	}
	if gotID != "nodeclaims" {
		t.Fatalf("expected List request id nodeclaims, got %q", gotID)
	}
	if len(got) != 1 {
		t.Fatalf("expected one flexvm agent pool, got %d", len(got))
	}
	if got[0].GetMetadata().GetId() != "flex" {
		t.Fatalf("expected flex agent pool, got %q", got[0].GetMetadata().GetId())
	}
}

func TestListByTypeReturnsErrorsForInvalidMatchingPayloads(t *testing.T) {
	flexAny := mustAny(t, flexvm.AgentPool_builder{}.Build())
	corruptFlexAny := &anypb.Any{
		TypeUrl: flexAny.GetTypeUrl(),
		Value:   []byte{0xff},
	}
	nebiusAny := mustAny(t, nebiusinstance.AgentPool_builder{}.Build())

	list := func(_ context.Context, _ *stretchapi.ListRequest, _ ...grpc.CallOption) (*stretchapi.ListResponse, error) {
		return stretchapi.ListResponse_builder{Items: []*anypb.Any{nebiusAny, corruptFlexAny}}.Build(), nil
	}

	if _, err := helper.ListByType[*flexvm.AgentPool](list, context.Background(), ""); err == nil {
		t.Fatal("expected matching corrupt flexvm payload to return an error")
	}
}

func mustAny(t *testing.T, msg proto.Message) *anypb.Any {
	t.Helper()
	item, err := anypb.New(msg)
	if err != nil {
		t.Fatalf("anypb.New: %v", err)
	}
	return item
}
