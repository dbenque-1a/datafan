package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	"github.com/dbenque/datafan/pkg/api"
	"github.com/dbenque/datafan/pkg/engine"
	"github.com/dbenque/datafan/pkg/store"

	"github.com/dbenque/datafan/pkg/grpc"
	"github.com/dbenque/datafan/pkg/grpc/model"
	google_protobuf "github.com/golang/protobuf/ptypes/timestamp"
	grpcFwk "google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {

	portPtr := flag.Int("port", 0, "port used for grpc server")
	publicAddrPtr := flag.String("public", "127.0.0.1", "public address for remote connection, defaulted to 127.0.0.1:port")
	idPtr := flag.String("id", "", "Identifier for the local server in the network of members.")
	remotePtr := flag.String("remote", "", "List of remote address to connect (comma separated)")

	flag.Parse()

	if portPtr == nil || *portPtr == 0 {
		fmt.Println("Missing port")
		os.Exit(1)
	}

	if idPtr == nil || *idPtr == "" {
		fmt.Println("Missing id")
		os.Exit(1)
	}

	portStr := fmt.Sprintf(":%d", *portPtr)
	lis, err := net.Listen("tcp", portStr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	grpcServer := grpcFwk.NewServer()

	public := *publicAddrPtr + portStr
	server := grpc.NewServer(*idPtr, public, store.NewMapStore())

	model.RegisterConnectorServer(grpcServer, server.GetConnector().Core().(*grpc.Connector))
	model.RegisterIndexMapCollectorServer(grpcServer, server.GetConnector().Core().(*grpc.Connector))
	model.RegisterDataRequestServer(grpcServer, server.GetConnector().Core().(*grpc.Connector))
	// Register reflection service on gRPC server.
	reflection.Register(grpcServer)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	for _, r := range strings.Split(*remotePtr, ",") {
		if r != "" {
			remoteMember := grpc.RemoteServer{
				Server: model.Server{Address: r},
			}
			server.GetConnector().Connect(&remoteMember)
		}
	}

	E := engine.NewEngine(server, time.Second)
	go E.Run(make(chan struct{}))

	var s1 = rand.NewSource(time.Now().UnixNano())
	var r1 = rand.New(s1)

	values := []string{
		"david",
		"cedric",
		"eric",
		"dario",
		"lenaic",
	}

	tick := time.NewTicker(time.Second)
	for range tick.C {
		fmt.Printf("Remotes: %#v\n", server.GetRemotes())
		fmt.Printf("Store: %s\n", server.DumpStore())
		t := time.Now()
		if r1.Intn(6)%5 == 0 {
			s := fmt.Sprintf("%s", values[r1.Intn(len(values))])
			v := fmt.Sprintf("%d", r1.Int63())
			item := grpc.Item{
				model.Item{
					Data:      []byte(v),
					Key:       s,
					Timestamp: &google_protobuf.Timestamp{Seconds: t.Unix(), Nanos: int32(t.Nanosecond())},
					Owner:     string(server.ID()),
				},
			}
			server.Put(api.Items{&item})
		}
	}
}
