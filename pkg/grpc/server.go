package grpc

import (
	"context"
	"fmt"

	"github.com/dbenque/datafan/pkg/api"
	"github.com/dbenque/datafan/pkg/engine"
	"github.com/dbenque/datafan/pkg/grpc/model"
	"github.com/dbenque/datafan/pkg/store"
	google_protobuf1 "github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

const (
	port = ":41120"
)

type Server struct {
	id        string
	storage   store.Interface
	connector api.Connector
	addr      string
}

var _ api.LocalMember = &Server{}

func NewServer(id string, publicAddress string, storage store.Interface) *Server {
	m := &Server{id: id, storage: storage, addr: publicAddress}
	m.connector = engine.NewConnector(m, newConnector)
	return m
}

func (s *Server) ID() api.ID {
	return api.ID(s.id)
}

func (s *Server) GetIndexes() api.IndexMap {
	im := api.IndexMap{
		Source:  s.ID(),
		Indexes: map[api.ID]api.Index{},
	}
	for _, id := range s.storage.GetMembers() {
		im.Indexes[id] = s.storage.GetIndex(id)
	}
	return im
}
func (s *Server) GetData(kps api.KeyIDPairs) api.Items {
	items := api.Items{}
	for _, kp := range kps {
		i := s.storage.Get(kp)
		if i == nil {
			continue
		}
		items = append(items, i)
	}
	return items
}
func (s *Server) Delete(kps api.KeyIDPairs) {
	s.storage.MultiDelete(kps)
}
func (s *Server) Put(items api.Items) {
	s.storage.MultiSet(items)
}
func (s *Server) GetConnector() api.Connector {
	return s.connector
}

func (s *Server) BackConnect(ctx context.Context, in *model.Server) (*google_protobuf1.Empty, error) {
	r := RemoteServer{Server: *in}
	fmt.Printf("BACKCONNECT in: %v\n", *in)
	if err := s.connector.Core().(*connector).RegisterAndDial(&r); err != nil {
		fmt.Printf("BACKCONNECT Error: %v", err)
		return nil, err
	}
	return nil, nil
}

func (s *Server) Who(context.Context, *google_protobuf1.Empty) (*model.Server, error) {
	fmt.Println("WHO")
	return &model.Server{
		Id:      s.id,
		Address: s.addr,
	}, nil
}

func (s *Server) GetRemotes() []string {
	return s.connector.Core().(*connector).GetRemotes()
}

// RemoteServer definition
type RemoteServer struct {
	model.Server
	conn *grpc.ClientConn
}

var _ api.Member = &RemoteServer{}

func (r *RemoteServer) ID() api.ID {
	return api.ID(r.Id)
}
