package grpc

import (
	"github.com/dbenque/datafan/pkg/engine"
	"github.com/dbenque/datafan/pkg/store"
)

const (
	port = ":41120"
)

type server struct {
	id      string
	storage store.Store
}

var _ engine.LocalMember = &server{}

func (s *server) ID() engine.ID {
	return engine.ID(s.id)
}

func (s *server) GetIndexes() engine.IndexMap {
	return engine.IndexMap{}
}
func (s *server) GetData(engine.KeyIDPairs) engine.Items {
	return engine.Items{}
}
func (s *server) Delete(kp engine.KeyIDPairs) {
	s.storage.MultiDelete(kp)
}
func (s *server) Put(items engine.Items) {
	s.storage.MultiSet(items)
}
func (s *server) GetConnector() engine.Connector {
	return nil
}

// func main() {

// 	lis, err := net.Listen("tcp", port)
// 	if err != nil {
// 		log.Fatalf("failed to listen: %v", err)
// 	}
// 	s := grpc.NewServer()
// 	model.RegisterIndexMapCollectorServer(s, &collector{})
// 	model.RegisterDataRequestServer(s, &dataRequestor{})
// 	// Register reflection service on gRPC server.
// 	reflection.Register(s)
// 	if err := s.Serve(lis); err != nil {
// 		log.Fatalf("failed to serve: %v", err)
// 	}
// }
