package flexvm

import (
	"context"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/Azure/aks-flex/plugin/api"
	"github.com/Azure/aks-flex/plugin/pkg/db"
	agentpools "github.com/Azure/aks-flex/plugin/pkg/services/agentpools/api"
)

var _ api.Object = (*Instance)(nil)

// Each AgentPool maps to exactly one VM, so the Instance API is a thin shim
// over the AgentPool — there is always one instance "<agentpool>/0".
type instancesServer struct {
	agentpools.UnimplementedInstancesServer
	storage db.RODB

	credentials azcore.TokenCredential
}

func NewInstancesServer(storage db.RODB) (agentpools.InstancesServer, error) {
	credentials, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}
	return &instancesServer{
		storage:     storage,
		credentials: credentials,
	}, nil
}

func (srv *instancesServer) List(ctx context.Context, req *api.ListRequest) (*api.ListResponse, error) {
	ap, ok := srv.storage.Get(req.GetId())
	if !ok {
		return nil, status.Error(codes.NotFound, "")
	}
	item, err := anypb.New(Instance_builder{
		Metadata: api.Metadata_builder{
			Id: to.Ptr(ap.GetMetadata().GetId() + "/0"),
		}.Build(),
	}.Build())
	if err != nil {
		return nil, err
	}
	return api.ListResponse_builder{
		Items: []*anypb.Any{item},
	}.Build(), nil
}

func (srv *instancesServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	ids := strings.Split(req.GetId(), "/")
	if len(ids) != 2 || ids[1] != "0" {
		return nil, status.Error(codes.NotFound, "")
	}
	if _, ok := srv.storage.Get(ids[0]); !ok {
		return nil, status.Error(codes.NotFound, "")
	}
	item, err := anypb.New(Instance_builder{
		Metadata: api.Metadata_builder{
			Id: to.Ptr(req.GetId()),
		}.Build(),
	}.Build())
	if err != nil {
		return nil, err
	}
	return api.GetResponse_builder{
		Item: item,
	}.Build(), nil
}
