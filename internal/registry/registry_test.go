package registry

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gamemesh-labs/gamemesh/internal/scheduler"
	"github.com/gamemesh-labs/gamemesh/pkg/model"
)

func TestRegisterHeartbeatAndImmutableSnapshot(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	r := New(Config{HeartbeatTTL: time.Second, Now: func() time.Time { return now }})
	if err := r.Register(model.GameServerSnapshot{ID: "gs-b", Region: "sg", Version: "v1", State: model.ServerReady, Capacity: 10}); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(model.GameServerSnapshot{ID: "gs-a", State: model.ServerStarting, Capacity: 5}); err != nil {
		t.Fatal(err)
	}
	before := r.Snapshot()
	if before.Len() != 2 || before.At(0).ID != "gs-a" || before.At(1).ID != "gs-b" {
		t.Fatalf("unexpected sorted snapshot: %#v", before)
	}
	if err := r.Heartbeat(Heartbeat{ServerID: "gs-b", State: model.ServerReady, CurrentPlayers: 4, Metrics: model.ServerMetrics{CPUPercent: 42}}); err != nil {
		t.Fatal(err)
	}
	after := r.Snapshot()
	if before.At(1).CurrentPlayers != 0 {
		t.Fatal("published snapshot was mutated by heartbeat")
	}
	if after.At(1).CurrentPlayers != 4 || after.At(1).Metrics.CPUPercent != 42 {
		t.Fatalf("heartbeat was not published: %#v", after.At(1))
	}
	if after.Generation() <= before.Generation() {
		t.Fatal("snapshot generation did not advance")
	}
}

func TestTTLMarksServerUnhealthyAndSchedulerExcludesIt(t *testing.T) {
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	r := New(Config{HeartbeatTTL: time.Second, Now: func() time.Time { return now }})
	for _, id := range []string{"expired", "healthy"} {
		if err := r.Register(model.GameServerSnapshot{ID: id, State: model.ServerReady, Capacity: 10}); err != nil {
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
	now = now.Add(2 * time.Second)
	if got := r.Sweep(); got != 1 {
		t.Fatalf("expired=%d want remaining healthy server", got)
	}
	_, err = scheduler.NewRoundRobin().Schedule(context.Background(), model.AllocationRequest{}, r.Snapshot())
	if !errors.Is(err, scheduler.ErrNoCandidate) {
		t.Fatalf("got %v want ErrNoCandidate", err)
	}
}

func TestHeartbeatValidatesKnownServerAndCapacity(t *testing.T) {
	r := New(Config{})
	if err := r.Heartbeat(Heartbeat{ServerID: "missing"}); !errors.Is(err, ErrServerNotFound) {
		t.Fatalf("got %v", err)
	}
	if err := r.Register(model.GameServerSnapshot{ID: "gs-1", Capacity: 2, State: model.ServerReady}); err != nil {
		t.Fatal(err)
	}
	if err := r.Heartbeat(Heartbeat{ServerID: "gs-1", CurrentPlayers: 3}); !errors.Is(err, ErrInvalidHeartbeat) {
		t.Fatalf("got %v", err)
	}
}

func TestConcurrentReadersAndWriters(t *testing.T) {
	r := New(Config{HeartbeatTTL: time.Hour})
	if err := r.Register(model.GameServerSnapshot{ID: "gs-1", State: model.ServerReady, Capacity: 100}); err != nil {
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
