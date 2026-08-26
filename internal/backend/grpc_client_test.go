package backend

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	gatewayv1 "game-gateway/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type grpcTestBackend struct {
	gatewayv1.UnimplementedBackendServiceServer
	handle func(context.Context, *gatewayv1.BackendRequest) (*gatewayv1.BackendResponse, error)
}

func (s grpcTestBackend) Handle(ctx context.Context, req *gatewayv1.BackendRequest) (*gatewayv1.BackendResponse, error) {
	return s.handle(ctx, req)
}

func startGRPCTestServer(t *testing.T, service gatewayv1.BackendServiceServer) *grpc.ClientConn {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	gatewayv1.RegisterBackendServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()

	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		server.Stop()
		_ = listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	})
	return conn
}

func TestGRPCClientMapsRequestAndResponse(t *testing.T) {
	var got *gatewayv1.BackendRequest
	conn := startGRPCTestServer(t, grpcTestBackend{handle: func(_ context.Context, req *gatewayv1.BackendRequest) (*gatewayv1.BackendResponse, error) {
		got = req
		return &gatewayv1.BackendResponse{MessageType: 1002, Payload: []byte("backend-ok")}, nil
	}})

	response, err := NewGRPCClient(conn).Handle(context.Background(), Request{
		UserID: "alice", SessionID: "session-1", RoomID: "room-1", MessageType: 1001,
		RequestID: "request-1", Payload: []byte("move"), TimestampUnixMS: 123,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.GetUserId() != "alice" || got.GetSessionId() != "session-1" || got.GetRoomId() != "room-1" || got.GetMessageType() != 1001 || got.GetRequestId() != "request-1" || string(got.GetPayload()) != "move" || got.GetTimestampUnixMs() != 123 {
		t.Fatalf("request was not mapped correctly: %#v", got)
	}
	if response.MessageType != 1002 || string(response.Payload) != "backend-ok" {
		t.Fatalf("response was not mapped correctly: %#v", response)
	}
}

func TestGRPCClientMapsUnavailable(t *testing.T) {
	conn := startGRPCTestServer(t, grpcTestBackend{handle: func(context.Context, *gatewayv1.BackendRequest) (*gatewayv1.BackendResponse, error) {
		return nil, status.Error(codes.Unavailable, "backend draining")
	}})
	_, err := NewGRPCClient(conn).Handle(context.Background(), Request{})
	if !errors.Is(err, ErrTransportUnavailable) {
		t.Fatalf("error = %v, want ErrTransportUnavailable", err)
	}
}

func TestGRPCClientHonorsCallerDeadline(t *testing.T) {
	conn := startGRPCTestServer(t, grpcTestBackend{handle: func(ctx context.Context, _ *gatewayv1.BackendRequest) (*gatewayv1.BackendResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	_, err := NewCaller(20*time.Millisecond).Call(context.Background(), NewGRPCClient(conn), Request{})
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("error = %v, want ErrTimeout", err)
	}
}
