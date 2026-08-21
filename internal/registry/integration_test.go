package registry_test

import (
	"context"
	"testing"
	"time"

	"github.com/gamemesh-labs/gamemesh/internal/registry"
	"github.com/gamemesh-labs/gamemesh/internal/scheduler"
	"github.com/gamemesh-labs/gamemesh/pkg/model"
)

func TestExpiredServerIsRemovedFromSchedulerCandidates(t *testing.T) {
	ctx := context.Background()
	reg := registry.NewInMemory(registry.Config{TTL: time.Millisecond})
	ready := func(id string) model.GameServerSnapshot {
		return model.GameServerSnapshot{ID: id, Region: "sg", Version: "v1", State: model.ServerReady, Capacity: 10}
	}
	if err := reg.Register(ctx, ready("healthy")); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(ctx, ready("will-expire")); err != nil {
		t.Fatal(err)
	}

	expiredEntry, _ := reg.Get("will-expire")
	// Refresh healthy after capturing the old expiry so only one server expires.
	if err := reg.Heartbeat(ctx, ready("healthy")); err != nil {
		t.Fatal(err)
	}
	reg.ExpireStale(expiredEntry.ExpiresAt.Add(time.Nanosecond))

	rr := scheduler.NewRoundRobin()
	source := registry.CandidateSource{Registry: reg}
	for i := 0; i < 5; i++ {
		result, err := rr.Schedule(ctx, model.AllocationRequest{Region: "sg"}, source.Candidates())
		if err != nil {
			t.Fatal(err)
		}
		if result.GameServerID != "healthy" {
			t.Fatalf("scheduled expired server %q", result.GameServerID)
		}
	}
}
