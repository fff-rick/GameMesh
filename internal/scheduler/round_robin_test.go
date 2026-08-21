package scheduler

import (
	"context"
	"testing"

	"github.com/gamemesh-labs/gamemesh/pkg/model"
)

func TestRoundRobinRotates(t *testing.T) {
	rr := NewRoundRobin()
	servers := []model.GameServerSnapshot{
		{ID: "a", State: model.ServerReady, Capacity: 10},
		{ID: "b", State: model.ServerReady, Capacity: 10},
		{ID: "c", State: model.ServerReady, Capacity: 10},
	}
	want := []string{"a", "b", "c", "a"}
	for i, expected := range want {
		got, err := rr.Schedule(context.Background(), model.AllocationRequest{}, servers)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if got.GameServerID != expected {
			t.Fatalf("iteration %d: got %s want %s", i, got.GameServerID, expected)
		}
	}
}

func TestRoundRobinFiltersUnavailableAndSemanticConstraints(t *testing.T) {
	rr := NewRoundRobin()
	servers := []model.GameServerSnapshot{
		{ID: "bad", Region: "sg", Version: "v1", State: model.ServerUnhealthy, Capacity: 10},
		{ID: "wrong-region", Region: "us", Version: "v1", State: model.ServerReady, Capacity: 10},
		{ID: "wrong-version", Region: "sg", Version: "v2", State: model.ServerReady, Capacity: 10},
		{ID: "good", Region: "sg", Version: "v1", State: model.ServerReady, Capacity: 10},
	}
	got, err := rr.Schedule(context.Background(), model.AllocationRequest{Region: "sg", Version: "v1"}, servers)
	if err != nil {
		t.Fatal(err)
	}
	if got.GameServerID != "good" {
		t.Fatalf("got %q want good", got.GameServerID)
	}
}

func TestRoundRobinNoCandidate(t *testing.T) {
	rr := NewRoundRobin()
	_, err := rr.Schedule(context.Background(), model.AllocationRequest{}, []model.GameServerSnapshot{{ID: "x", State: model.ServerDraining, Capacity: 10}})
	if err != ErrNoCandidate {
		t.Fatalf("got %v want ErrNoCandidate", err)
	}
}
