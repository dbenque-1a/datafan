package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/dbenque/datafan/pkg/store"

	"github.com/dbenque/datafan/pkg/grpc"
	"github.com/dbenque/datafan/pkg/grpc/model"
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

	model.RegisterConnectorServer(grpcServer, server)

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

	tick := time.NewTicker(time.Second)
	for range tick.C {
		fmt.Printf("Remotes: %#v\n", server.GetRemotes())
	}
}
