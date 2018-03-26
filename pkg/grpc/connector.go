package grpc

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dbenque/datafan/pkg/api"
	"github.com/dbenque/datafan/pkg/grpc/model"
	google_protobuf1 "github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

type connector struct {
	localMember    *Server
	remoteHandling sync.RWMutex
	remoteMember   map[api.ID]api.Member
	connectorChan  api.ConnectorChan
}

var _ api.ConnectorCore = &connector{}

func newConnector(localMember api.LocalMember, connectorChan api.ConnectorChan) api.ConnectorCore {
	return &connector{
		localMember:   localMember.(*Server),
		remoteMember:  map[api.ID]api.Member{},
		connectorChan: connectorChan,
	}
}

func (c *connector) GetLocalMember() api.LocalMember {
	return c.localMember
}

func (c *connector) Connect(m api.Member) error {
	if err := c.RegisterAndDial(m); err != nil {
		return err
	}
	mm, _ := m.(*RemoteServer)

	client := model.NewConnectorClient(mm.conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	server := model.Server{
		Id:      c.localMember.id,
		Address: c.localMember.addr,
	}
	if _, err := client.BackConnect(ctx, &server); err != nil {
		return err
	}
	return nil
}

func (c *connector) RegisterAndDial(m api.Member) error {
	c.remoteHandling.Lock()
	defer c.remoteHandling.Unlock()
	mm, ok := m.(*RemoteServer)
	if !ok {
		return fmt.Errorf("unknown remote memeber type")
	}

	var err error
	fmt.Printf("grpcDial: %s\n", mm.Address)
	con, err := grpc.Dial(mm.Address, grpc.WithInsecure())
	if err != nil {
		return fmt.Errorf("did not connect: %v", err)
	}
	client := model.NewConnectorClient(con)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	who, err := client.Who(ctx, &google_protobuf1.Empty{})
	if err != nil {
		return err
	}

	mm.Server.Id = who.Id
	mm.conn = con
	c.remoteMember[mm.ID()] = mm
	return nil
}

func (c *connector) ProcessIndexMap(index api.IndexMap) {
	c.remoteHandling.RLock()
	defer c.remoteHandling.RUnlock()
	// for _, m := range c.remoteMember {
	// 	//TODO GRPC To send index to all remote servers
	// 	//Push Response in ReceiveIndexCh
	// }
}

func (c *connector) ProcessDataRequest(rq api.DataRequest) {
	log.Fatalf("ProcessDataRequest should not happen in a RPC Context")
}

func (c *connector) ForwardDataRequest(rq api.DataRequest) {
	c.remoteHandling.RLock()
	defer c.remoteHandling.RUnlock()
	//	m := c.remoteMember[rq.RequestSource]
	//TODO RPC Call to rq.Destination
	//Push Response in ReceiveDataCh
}

func (c *connector) GetRemotes() []string {
	remotes := []string{}
	for k := range c.remoteMember {
		remotes = append(remotes, string(k))
	}
	return remotes
}
