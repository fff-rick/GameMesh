package registry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gamemesh-labs/gamemesh/internal/scheduler"
	"github.com/gamemesh-labs/gamemesh/pkg/model"
)

func testServer(id string, state model.ServerState) model.GameServerSnapshot {
	return model.GameServerSnapshot{ID: id, Address: "127.0.0.1:7001", Region: "sg", Zone: "a", Version: "v1", State: state, Capacity: 100}
}

func TestRegisterHeartbeatAndImmutableSnapshot(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	r := New(Config{HeartbeatTTL: time.Second, Now: func() time.Time { return now }})
	if err := r.Register(testServer("gs-b", model.ServerReady)); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(testServer("gs-a", model.ServerStarting)); err != nil {
		t.Fatal(err)
	}
	before := r.Snapshot()
	if before.Len() != 2 || before.At(0).ID != "gs-a" || before.At(1).ID != "gs-b" {
		t.Fatalf("unexpected sorted snapshot")
	}
	if err := r.Heartbeat(Heartbeat{ServerID: "gs-b", State: model.ServerReady, CurrentPlayers: 4, Metrics: model.ServerMetrics{CPUPercent: 42}}); err != nil {
		t.Fatal(err)
	}
	if before.At(1).CurrentPlayers != 0 {
		t.Fatal("published snapshot was mutated")
	}
	if r.Snapshot().Generation() != before.Generation() {
		t.Fatal("metric-only heartbeat should wait for batch publication")
	}
	if !r.PublishPending(now) || r.Snapshot().At(1).CurrentPlayers != 4 {
		t.Fatal("heartbeat update was not published")
	}
}

func TestHeartbeatCannotChangeRegistrationAttributes(t *testing.T) {
	r := New(Config{})
	server := testServer("gs-1", model.ServerReady)
	if err := r.Register(server); err != nil {
		t.Fatal(err)
	}
	if err := r.Heartbeat(Heartbeat{ServerID: "gs-1", CurrentPlayers: 1}); err != nil {
		t.Fatal(err)
	}
	got := r.Snapshot().At(0)
	if got.Address != server.Address || got.Region != server.Region || got.Zone != server.Zone || got.Version != server.Version || got.Capacity != server.Capacity {
		t.Fatalf("heartbeat changed static attributes: %#v", got)
	}
}

func TestTTLMarksServerUnhealthyAndSchedulerExcludesIt(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	r := New(Config{HeartbeatTTL: time.Second, Now: func() time.Time { return now }})
	for _, id := range []string{"expired", "healthy"} {
		if err := r.Register(testServer(id, model.ServerReady)); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(500 * time.Millisecond)
	if err := r.Heartbeat(Heartbeat{ServerID: "healthy", State: model.ServerReady}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(600 * time.Millisecond)
	if got := r.Sweep(); got != 1 {
		t.Fatalf("expired=%d want 1", got)
	}
	selected, err := scheduler.NewRoundRobin().Schedule(context.Background(), model.AllocationRequest{}, r.Snapshot())
	if err != nil || selected.GameServerID != "healthy" {
		t.Fatalf("selected=%+v err=%v", selected, err)
	}
}

func TestHeartbeatValidatesKnownServerAndCapacity(t *testing.T) {
	r := New(Config{})
	if err := r.Heartbeat(Heartbeat{ServerID: "missing"}); !errors.Is(err, ErrServerNotFound) {
		t.Fatalf("got %v", err)
	}
	server := testServer("gs-1", model.ServerReady)
	server.Capacity = 2
	if err := r.Register(server); err != nil {
		t.Fatal(err)
	}
	if err := r.Heartbeat(Heartbeat{ServerID: "gs-1", CurrentPlayers: 3}); !errors.Is(err, ErrInvalidHeartbeat) {
		t.Fatalf("got %v", err)
	}
}

func TestTerminatedServerIsNotRewrittenByExpiry(t *testing.T) {
	now := time.Now()
	r := New(Config{HeartbeatTTL: time.Millisecond, Now: func() time.Time { return now }})
	if err := r.Register(testServer("gs-1", model.ServerTerminated)); err != nil {
		t.Fatal(err)
	}
	r.ExpireStale(now.Add(time.Hour))
	if got := r.Snapshot().At(0).State; got != model.ServerTerminated {
		t.Fatalf("got %s want TERMINATED", got)
	}
}

func TestDeregisterRemovesServer(t *testing.T) {
	r := New(Config{})
	if err := r.Register(testServer("gs-1", model.ServerReady)); err != nil {
		t.Fatal(err)
	}
	if err := r.Deregister("gs-1"); err != nil || r.Snapshot().Len() != 0 {
		t.Fatalf("deregister err=%v len=%d", err, r.Snapshot().Len())
	}
}

func TestConcurrentReadersAndWriters(t *testing.T) {
	r := New(Config{HeartbeatTTL: time.Hour})
	if err := r.Register(testServer("gs-1", model.ServerReady)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 500; n++ {
				snapshot := r.Snapshot()
				for j := 0; j < snapshot.Len(); j++ {
					_ = snapshot.At(j)
				}
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for n := 0; n < 500; n++ {
				if err := r.Heartbeat(Heartbeat{ServerID: "gs-1", State: model.ServerReady, CurrentPlayers: (n + offset) % 100}); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}
	wg.Wait()
}

var benchmarkSnapshotSink *Snapshot

func BenchmarkSnapshotRead1000Servers(b *testing.B) {
	r := New(Config{})
	for i := 0; i < 1000; i++ {
		_ = r.Register(testServer(fmt.Sprintf("gs-%04d", i), model.ServerReady))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkSnapshotSink = r.Snapshot()
	}
}

func BenchmarkHeartbeatAndPublish1000Servers(b *testing.B) {
	r := New(Config{})
	for i := 0; i < 1000; i++ {
		_ = r.Register(testServer(fmt.Sprintf("gs-%04d", i), model.ServerReady))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = r.Heartbeat(Heartbeat{ServerID: "gs-0000", State: model.ServerReady, CurrentPlayers: i % 100})
		r.PublishPending(time.Now())
	}
}
