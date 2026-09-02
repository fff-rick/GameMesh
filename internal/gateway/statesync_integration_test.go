package gateway

import (
	"context"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"game-gateway/internal/config"
	"game-gateway/internal/protocol"
	"game-gateway/internal/routing"
	"game-gateway/internal/statesync"
	state_syncv1 "github.com/xin/gsss/api/state_sync/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type recordingStateSync struct {
	state_syncv1.UnimplementedStateSyncGatewayServer
	joined chan *state_syncv1.PlayerJoin
	input  chan *state_syncv1.PlayerInput
}

func (s *recordingStateSync) Connect(stream state_syncv1.StateSyncGateway_ConnectServer) error {
	join, err := stream.Recv()
	if err != nil {
		return err
	}
	s.joined <- join.GetPlayerJoin()
	input, err := stream.Recv()
	if err != nil {
		return err
	}
	s.input <- input.GetPlayerInput()
	return stream.Send(&state_syncv1.StateSyncToGateway{Message: &state_syncv1.StateSyncToGateway_Snapshot{Snapshot: &state_syncv1.Snapshot{MatchId: input.GetPlayerInput().GetMatchId(), RecipientPlayerId: input.GetPlayerInput().GetPlayerId(), SnapshotId: 1, ServerTick: 1}}})
}

func TestGameMeshRoutesTrustedInputToStateSyncAndReturnsSnapshot(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	service := &recordingStateSync{joined: make(chan *state_syncv1.PlayerJoin, 1), input: make(chan *state_syncv1.PlayerInput, 1)}
	grpcServer := grpc.NewServer()
	state_syncv1.RegisterStateSyncGatewayServer(grpcServer, service)
	go grpcServer.Serve(listener)
	t.Cleanup(func() { grpcServer.Stop(); listener.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := grpc.DialContext(ctx, "bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	bridge, err := statesync.New(ctx, conn, statesync.Handler{})
	if err != nil {
		t.Fatal(err)
	}

	router := routing.NewStaticRouter()
	router.SetMessageBackend(statesync.MessageTypeInput, "state-sync")
	router.SetUserRoom("alice", "match-a")
	router.SetRoomInstance("match-a", routing.BackendInstance{ID: "state-sync", BackendType: "state-sync"})
	cfg := config.Default()
	gw := New(cfg, "test-gateway", testLogger(), WithAuthenticator(tokenAuthenticator{"token": "alice"}), WithRouter(router), WithStateSyncClient(bridge))
	ts := httptest.NewServer(gw.Handler())
	t.Cleanup(func() { gw.Close(); ts.Close() })
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	client := dial(t, url)
	defer client.Close()
	if result := sendAuth(t, client, "token"); !result.OK {
		t.Fatalf("auth=%#v", result)
	}
	payload, err := proto.Marshal(&state_syncv1.PlayerInput{MatchId: "forged", PlayerId: 999, InputSeq: 999, DirectionX: 1})
	if err != nil {
		t.Fatal(err)
	}
	writeEnvelope(t, client, protocol.Envelope{Version: protocol.CurrentVersion, MessageType: statesync.MessageTypeInput, RequestID: "move", Payload: payload})
	response := readEnvelopeWithin(t, client, time.Second)
	if response.MessageType != statesync.MessageTypeSnapshot {
		t.Fatalf("response=%#v", response)
	}
	var snapshot state_syncv1.Snapshot
	if err := proto.Unmarshal(response.Payload, &snapshot); err != nil {
		t.Fatal(err)
	}
	playerID := stateSyncPlayerID("alice")
	if snapshot.MatchId != "match-a" || snapshot.RecipientPlayerId != playerID {
		t.Fatalf("snapshot match=%q recipient=%d", snapshot.MatchId, snapshot.RecipientPlayerId)
	}
	select {
	case join := <-service.joined:
		if join.MatchId != "match-a" || join.PlayerId != playerID {
			t.Fatalf("join=%#v", join)
		}
	case <-time.After(time.Second):
		t.Fatal("missing join")
	}
	select {
	case input := <-service.input:
		if input.MatchId != "match-a" || input.PlayerId != playerID || input.InputSeq != 1 {
			t.Fatalf("input=%#v", input)
		}
	case <-time.After(time.Second):
		t.Fatal("missing input")
	}
}
