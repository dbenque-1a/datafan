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

type Connector struct {
	localMember    *Server
	remoteHandling sync.RWMutex
	remoteMember   map[api.ID]api.Member
	connectorChan  api.ConnectorChan
}

var _ api.ConnectorCore = &Connector{}

func newConnector(localMember api.LocalMember, connectorChan api.ConnectorChan) api.ConnectorCore {
	return &Connector{
		localMember:   localMember.(*Server),
		remoteMember:  map[api.ID]api.Member{},
		connectorChan: connectorChan,
	}
}

func (c *Connector) GetLocalMember() api.LocalMember {
	return c.localMember
}

func (c *Connector) Connect(m api.Member) error {
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

func (c *Connector) RegisterAndDial(m api.Member) error {
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

func (c *Connector) Who(context.Context, *google_protobuf1.Empty) (*model.Server, error) {
	fmt.Println("WHO")
	s := c.localMember
	return &model.Server{
		Id:      s.id,
		Address: s.addr,
	}, nil
}

func (c *Connector) BackConnect(ctx context.Context, in *model.Server) (*google_protobuf1.Empty, error) {
	fmt.Println("BackConnect")
	r := RemoteServer{Server: *in}
	if err := c.RegisterAndDial(&r); err != nil {
		return nil, err
	}
	return &google_protobuf1.Empty{}, nil
}

func (c *Connector) CollectIndexMap(ctx context.Context, in *model.IndexMap) (*google_protobuf1.Empty, error) {
	indexMap := ConvertIndexMapGRPCModelToAPI(in)
	c.connectorChan.ReceiveIndexMap(indexMap)
	return &google_protobuf1.Empty{}, nil
}

func (c *Connector) GetData(ctx context.Context, kps *model.KeyIDPairs) (*model.DataResponse, error) {
	return &model.DataResponse{Items: ConvertItemsAPItoModel(c.localMember.GetData(ConvertKeyIDPairsModelToAPI(kps)))}, nil
}

func (c *Connector) FanOutIndexMap(index api.IndexMap) {
	in := ConvertIndexMapApiToGRPCModel(&index)
	c.remoteHandling.RLock()
	defer c.remoteHandling.RUnlock()
	for _, m := range c.remoteMember {
		mm, ok := m.(*RemoteServer)
		if !ok {
			log.Printf("[FanOutIndexMap] unknown remote memeber type")
		}
		go func(r *RemoteServer) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			client := model.NewIndexMapCollectorClient(r.conn)
			_, err := client.CollectIndexMap(ctx, in)
			if err != nil {
				log.Printf("[FanOutIndexMap] fail toward member %s", r.Id)
			}
		}(mm)
	}
}

func (c *Connector) ProcessDataRequest(rq api.DataRequest) {
	log.Fatalf("ProcessDataRequest should not happen in a RPC Context")
}

func (c *Connector) ForwardDataRequest(rq api.DataRequest) {
	c.remoteHandling.RLock()
	defer c.remoteHandling.RUnlock()
	m, found := c.remoteMember[rq.RequestDestination]
	if !found {
		log.Printf("[ForwardDataRequest] remote %s is not connected for the moment", rq.RequestDestination)
		return
	}
	mm, ok := m.(*RemoteServer)
	if !ok {
		log.Fatalf("[ForwardDataRequest] unknown remote memeber type")
	}

	go func(r *RemoteServer) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		client := model.NewDataRequestClient(r.conn)
		kps := ConvertKeyIDPairsAPItoModel(&rq.KeyIDPairs)
		data, err := client.GetData(ctx, kps)
		if err != nil {
			log.Printf("[ForwardDataRequest] fail to fetch data from member %s", r.Id)
			return
		}
		data.AssociatedBuildTime = ConvertAssociatedBuildTimeAPIToModel(rq.AssociatedBuildTime)
		c.connectorChan.ReceiveData(ConvertDataResponseModelToAPI(data))
	}(mm)
}

func (c *Connector) GetRemotes() []string {
	remotes := []string{}
	for k := range c.remoteMember {
		remotes = append(remotes, string(k))
	}
	return remotes
}
