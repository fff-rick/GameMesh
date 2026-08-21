package registry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gamemesh-labs/gamemesh/pkg/model"
)

func testServer(id string, state model.ServerState) model.GameServerSnapshot {
	return model.GameServerSnapshot{ID: id, Region: "sg", Version: "v1", State: state, Capacity: 100}
}

func TestRegisterPublishesSortedSnapshot(t *testing.T) {
	r := NewInMemory(Config{TTL: time.Second})
	ctx := context.Background()
	for _, id := range []string{"c", "a", "b"} {
		if err := r.Register(ctx, testServer(id, model.ServerReady)); err != nil {
			t.Fatal(err)
		}
	}

	view := r.Snapshot()
	if len(view.Servers) != 3 {
		t.Fatalf("got %d servers want 3", len(view.Servers))
	}
	got := []string{view.Servers[0].ID, view.Servers[1].ID, view.Servers[2].ID}
	want := []string{"a", "b", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got order %v want %v", got, want)
		}
	}
}

func TestExpireStaleMarksUnhealthyAndHeartbeatRecovers(t *testing.T) {
	ttl := 100 * time.Millisecond
	r := NewInMemory(Config{TTL: ttl})
	ctx := context.Background()
	s := testServer("gs-1", model.ServerReady)
	if err := r.Register(ctx, s); err != nil {
		t.Fatal(err)
	}

	entry, ok := r.Get("gs-1")
	if !ok {
		t.Fatal("server missing")
	}
	if changed := r.ExpireStale(entry.ExpiresAt.Add(time.Nanosecond)); changed != 1 {
		t.Fatalf("expired %d servers want 1", changed)
	}
	if got := r.Snapshot().Servers[0].State; got != model.ServerUnhealthy {
		t.Fatalf("got state %s want %s", got, model.ServerUnhealthy)
	}

	s.State = model.ServerReady
	if err := r.Heartbeat(ctx, s); err != nil {
		t.Fatal(err)
	}
	if got := r.Snapshot().Servers[0].State; got != model.ServerReady {
		t.Fatalf("got recovered state %s want %s", got, model.ServerReady)
	}
}

func TestHeartbeatUnknownServer(t *testing.T) {
	r := NewInMemory(Config{})
	err := r.Heartbeat(context.Background(), testServer("missing", model.ServerReady))
	if !errors.Is(err, ErrNotRegistered) {
		t.Fatalf("got %v want ErrNotRegistered", err)
	}
}

func TestTerminatedServerIsNotRewrittenByExpiry(t *testing.T) {
	r := NewInMemory(Config{TTL: time.Millisecond})
	ctx := context.Background()
	if err := r.Register(ctx, testServer("gs-1", model.ServerTerminated)); err != nil {
		t.Fatal(err)
	}
	entry, _ := r.Get("gs-1")
	r.ExpireStale(entry.ExpiresAt.Add(time.Hour))
	if got := r.Snapshot().Servers[0].State; got != model.ServerTerminated {
		t.Fatalf("got %s want TERMINATED", got)
	}
}

func TestDeregisterRemovesServer(t *testing.T) {
	r := NewInMemory(Config{})
	ctx := context.Background()
	if err := r.Register(ctx, testServer("gs-1", model.ServerReady)); err != nil {
		t.Fatal(err)
	}
	if err := r.Deregister(ctx, "gs-1"); err != nil {
		t.Fatal(err)
	}
	if got := len(r.Snapshot().Servers); got != 0 {
		t.Fatalf("got %d servers want 0", got)
	}
}

func TestMetricHeartbeatIsBatchedUntilPublish(t *testing.T) {
	r := NewInMemory(Config{TTL: time.Second, PublishInterval: time.Second})
	ctx := context.Background()
	s := testServer("gs-1", model.ServerReady)
	if err := r.Register(ctx, s); err != nil {
		t.Fatal(err)
	}
	before := r.Snapshot().Revision

	s.Metrics.CPUPercent = 77
	if err := r.Heartbeat(ctx, s); err != nil {
		t.Fatal(err)
	}
	if got := r.Snapshot().Revision; got != before {
		t.Fatalf("metric-only heartbeat published immediately: revision=%d want=%d", got, before)
	}
	if !r.PublishPending(time.Now()) {
		t.Fatal("expected pending heartbeat update")
	}
	view := r.Snapshot()
	if view.Servers[0].Metrics.CPUPercent != 77 {
		t.Fatalf("cpu=%v want=77", view.Servers[0].Metrics.CPUPercent)
	}
}

func TestConcurrentMutationAndSnapshotRead(t *testing.T) {
	r := NewInMemory(Config{TTL: time.Second})
	ctx := context.Background()
	const n = 32
	for i := 0; i < n; i++ {
		if err := r.Register(ctx, testServer(fmt.Sprintf("gs-%02d", i), model.ServerReady)); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(offset int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				id := fmt.Sprintf("gs-%02d", (i+offset)%n)
				s := testServer(id, model.ServerAllocated)
				s.CurrentPlayers = i % 50
				if err := r.Heartbeat(ctx, s); err != nil {
					t.Errorf("heartbeat: %v", err)
					return
				}
				view := r.Snapshot()
				if len(view.Servers) != n {
					t.Errorf("snapshot len=%d want=%d", len(view.Servers), n)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
}

var benchmarkViewSink *View

func BenchmarkSnapshotRead1000Servers(b *testing.B) {
	r := NewInMemory(Config{})
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		_ = r.Register(ctx, testServer(fmt.Sprintf("gs-%04d", i), model.ServerReady))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkViewSink = r.Snapshot()
	}
	if len(benchmarkViewSink.Servers) != 1000 {
		b.Fatal("unexpected view size")
	}
}

func BenchmarkHeartbeatUpdate1000Servers(b *testing.B) {
	r := NewInMemory(Config{})
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		_ = r.Register(ctx, testServer(fmt.Sprintf("gs-%04d", i), model.ServerReady))
	}
	server := testServer("gs-0000", model.ServerReady)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.CurrentPlayers = i % 100
		if err := r.Heartbeat(ctx, server); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPublish1000Servers(b *testing.B) {
	r := NewInMemory(Config{})
	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		_ = r.Register(ctx, testServer(fmt.Sprintf("gs-%04d", i), model.ServerReady))
	}
	server := testServer("gs-0000", model.ServerReady)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		server.CurrentPlayers = i % 100
		b.StopTimer()
		if err := r.Heartbeat(ctx, server); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		r.PublishPending(time.Now())
	}
}
