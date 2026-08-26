package main

import (
	"context"
	"flag"
	"fmt"
	gatewayv1 "game-gateway/api/proto"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
)

type fakeBackend struct {
	gatewayv1.UnimplementedBackendServiceServer
	mode string // success | delay | business-error
}

func (s *fakeBackend) Handle(
	ctx context.Context,
	req *gatewayv1.BackendRequest,
) (*gatewayv1.BackendResponse, error) {
	switch s.mode {
	case "delay":
		<-ctx.Done()
		return nil, ctx.Err()
	case "business-error":
		return &gatewayv1.BackendResponse{
			ErrorCode: "room_full",
		}, nil
	default:
		return &gatewayv1.BackendResponse{
			MessageType: 1002,
			Payload:     append([]byte(nil), req.Payload...),
		}, nil
	}
}

func main() {
	listenAddr := flag.String("listen", "127.0.0.1:0", "gRPC listen address")
	mode := flag.String("mode", "success", "success, delay, or business-error")
	flag.Parse()

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer()
	gatewayv1.RegisterBackendServiceServer(server, &fakeBackend{mode: *mode})

	// The first line is intentionally machine-readable for black-box tests.
	fmt.Println(listener.Addr().String())
	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf("gRPC server stopped: %v", err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	server.GracefulStop()
}
