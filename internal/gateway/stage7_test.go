package gateway

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"game-gateway/internal/backend"
	"game-gateway/internal/config"
	"game-gateway/internal/protocol"
	"game-gateway/internal/ws"
)

func stage7Metric(t *testing.T, gw *Server) string {
	t.Helper()
	recorder := httptest.NewRecorder()
	gw.Handler().ServeHTTP(recorder, httptest.NewRequest("GET", "/metrics", nil))
	return recorder.Body.String()
}

func TestConnectionRateLimitDropsFloodWithoutClosingOtherConnections(t *testing.T) {
	cfg := config.Default()
	cfg.ConnectionRate = 1
	cfg.ConnectionRateBurst = 1
	cfg.GlobalRate = 1000
	cfg.GlobalRateBurst = 1000
	gw := New(cfg, "test-gateway", testLogger())
	ts := httptest.NewServer(gw.Handler())
	t.Cleanup(func() { gw.Close(); ts.Close() })
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	flooder := dial(t, url)
	defer flooder.Close()
	for i := 0; i < 5; i++ {
		writeEnvelope(t, flooder, protocol.Envelope{Version: protocol.CurrentVersion, MessageType: protocol.MessageTypeEchoRequest, RequestID: "flood"})
	}
	waitFor(t, time.Second, func() bool { return strings.Contains(stage7Metric(t, gw), `scope="connection"`) })
	peer := dial(t, url)
	defer peer.Close()
	// Its independent bucket still admits its first message.
	echo(t, peer, "peer", []byte("ok"))
}

func TestGlobalRateLimitBoundsBurstAcrossConnections(t *testing.T) {
	cfg := config.Default()
	cfg.ConnectionRate, cfg.ConnectionRateBurst = 1000, 1000
	cfg.GlobalRate, cfg.GlobalRateBurst = 1, 1
	gw := New(cfg, "test-gateway", testLogger())
	ts := httptest.NewServer(gw.Handler())
	t.Cleanup(func() { gw.Close(); ts.Close() })
	url := strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
	first, second := dial(t, url), dial(t, url)
	defer first.Close()
	defer second.Close()
	writeEnvelope(t, first, protocol.Envelope{Version: protocol.CurrentVersion, MessageType: protocol.MessageTypeEchoRequest, RequestID: "one"})
	writeEnvelope(t, second, protocol.Envelope{Version: protocol.CurrentVersion, MessageType: protocol.MessageTypeEchoRequest, RequestID: "two"})
	waitFor(t, time.Second, func() bool { return strings.Contains(stage7Metric(t, gw), `scope="global"`) })
}

func TestBackendInFlightLimitFailsFast(t *testing.T) {
	gw, client, _, _ := setupRoutedServer(t, fakeBackendClient{handle: func(context.Context, backend.Request) (backend.Response, error) {
		return backend.Response{MessageType: 1002}, nil
	}})
	gw.backendSlots = make(chan struct{}, 1)
	gw.backendSlots <- struct{}{}
	defer func() { <-gw.backendSlots }()
	response := sendBusiness(t, client, 1001, "overloaded", nil)
	assertErrorEnvelope(t, response, "backend_overloaded", true)
	if !strings.Contains(stage7Metric(t, gw), "game_gateway_backend_rejected_total") {
		t.Fatal("missing backend backpressure metric")
	}
}

func TestDrainStopsAcceptingAndForcesConnectionsAtDeadline(t *testing.T) {
	gw, ts, url := newTestServer(t)
	client := dial(t, url)
	defer client.Close()
	if !gw.BeginDrain() {
		t.Fatal("first drain should start")
	}
	if _, err := ws.Dial(url, 100*time.Millisecond); err == nil {
		t.Fatal("new connection accepted during drain")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := gw.Drain(ctx); err == nil {
		t.Fatal("expected drain deadline")
	}
	if gw.ConnectionCount() != 0 {
		t.Fatalf("connections=%d", gw.ConnectionCount())
	}
	if !strings.Contains(stage7Metric(t, gw), `result="drain_timeout"`) {
		t.Fatal("missing drain timeout metric")
	}
	_ = ts
}
