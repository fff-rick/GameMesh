package simulator

import (
	"errors"
	"testing"

	"github.com/gamemesh-labs/gamemesh/pkg/model"
)

func TestGameServerCapacityAndRelease(t *testing.T) {
	s := NewGameServer(ServerConfig{ID: "gs-1", Capacity: 2})
	if err := s.Allocate(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Allocate(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Allocate(1); !errors.Is(err, ErrServerFull) {
		t.Fatalf("got %v want ErrServerFull", err)
	}
	if got := s.Snapshot().CurrentPlayers; got != 2 {
		t.Fatalf("players=%d want 2", got)
	}
	s.Release(1)
	if got := s.Snapshot().CurrentPlayers; got != 1 {
		t.Fatalf("players=%d want 1", got)
	}
}

func TestUnhealthyServerRejectsAllocation(t *testing.T) {
	s := NewGameServer(ServerConfig{ID: "gs-1", Capacity: 10})
	s.SetState(model.ServerUnhealthy)
	if err := s.Allocate(1); !errors.Is(err, ErrServerUnavailable) {
		t.Fatalf("got %v want ErrServerUnavailable", err)
	}
}

func TestMetricsIncreaseWithLoad(t *testing.T) {
	s := NewGameServer(ServerConfig{ID: "gs-1", Capacity: 100, BaseCPUPercent: 5, BaseTickMillis: 10})
	before := s.Snapshot()
	if err := s.Allocate(90); err != nil {
		t.Fatal(err)
	}
	after := s.Snapshot()
	if after.Metrics.CPUPercent <= before.Metrics.CPUPercent {
		t.Fatalf("cpu did not increase: before %.2f after %.2f", before.Metrics.CPUPercent, after.Metrics.CPUPercent)
	}
	if after.Metrics.TickLatencyMillis <= before.Metrics.TickLatencyMillis {
		t.Fatalf("tick latency did not increase")
	}
}
