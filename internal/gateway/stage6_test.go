package gateway

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"game-gateway/internal/config"
	"game-gateway/internal/presence"
)

func newPresenceTestServer(t *testing.T, id string, registry presence.Registry) (*Server, *httptest.Server, string) {
	t.Helper()
	cfg := config.Default()
	cfg.SendQueueSize = 8
	cfg.PresenceLeaseTTL = 100 * time.Millisecond
	cfg.PresenceRenewInterval = 20 * time.Millisecond
	cfg.PresenceOperationTimeout = 50 * time.Millisecond
	gw := New(cfg, id, testLogger(), WithPresenceRegistry(registry))
	ts := httptest.NewServer(gw.Handler())
	t.Cleanup(func() { gw.Close(); ts.Close() })
	return gw, ts, strings.Replace(ts.URL, "http://", "ws://", 1) + "/ws"
}

func TestCrossGatewayDuplicateLoginEvictsPreviousConnection(t *testing.T) {
	registry := presence.NewMemoryRegistry(time.Second)
	gwA, _, urlA := newPresenceTestServer(t, "gateway-a", registry)
	gwB, _, urlB := newPresenceTestServer(t, "gateway-b", registry)
	clientA := dial(t, urlA)
	defer clientA.Close()
	if result := sendAuth(t, clientA, "user:alice"); !result.OK {
		t.Fatalf("A auth=%#v", result)
	}
	clientB := dial(t, urlB)
	defer clientB.Close()
	if result := sendAuth(t, clientB, "user:alice"); !result.OK {
		t.Fatalf("B auth=%#v", result)
	}
	waitFor(t, time.Second, func() bool { return gwA.ConnectionCount() == 0 })
	if gwB.ConnectionCount() != 1 {
		t.Fatalf("gateway B connections=%d", gwB.ConnectionCount())
	}
}

func TestRegistryOutageRejectsNewAuthButKeepsExistingConnection(t *testing.T) {
	registry := presence.NewMemoryRegistry(100 * time.Millisecond)
	_, _, url := newPresenceTestServer(t, "gateway-a", registry)
	client := dial(t, url)
	defer client.Close()
	if result := sendAuth(t, client, "user:alice"); !result.OK {
		t.Fatalf("auth=%#v", result)
	}
	registry.SetAvailable(false)
	time.Sleep(150 * time.Millisecond)
	echo(t, client, "still-open", []byte("ok"))
	newClient := dial(t, url)
	defer newClient.Close()
	if result := sendAuth(t, newClient, "user:bob"); result.OK || result.ErrorCode != "presence_unavailable" {
		t.Fatalf("auth during outage=%#v", result)
	}
}
