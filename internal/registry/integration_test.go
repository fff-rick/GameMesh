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
	now := time.Date(2026, 8, 21, 12, 0, 0, 0, time.UTC)
	reg := registry.New(registry.Config{HeartbeatTTL: time.Millisecond, Now: func() time.Time { return now }})
	ready := func(id string) model.GameServerSnapshot {
		return model.GameServerSnapshot{ID: id, Region: "sg", Version: "v1", State: model.ServerReady, Capacity: 10}
	}
	if err := reg.Register(ready("healthy")); err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(ready("will-expire")); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Millisecond / 2)
	if err := reg.Heartbeat(registry.Heartbeat{ServerID: "healthy", State: model.ServerReady}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Millisecond/2 + time.Nanosecond)
	reg.Sweep()

	rr := scheduler.NewRoundRobin()
	source := registry.CandidateSource{Registry: reg}
	result, err := rr.Schedule(context.Background(), model.AllocationRequest{Region: "sg"}, source.Candidates())
	if err != nil || result.GameServerID != "healthy" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
