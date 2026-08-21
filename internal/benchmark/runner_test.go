package benchmark

import (
	"context"
	"testing"

	"github.com/gamemesh-labs/gamemesh/internal/registry"
	"github.com/gamemesh-labs/gamemesh/internal/scheduler"
	"github.com/gamemesh-labs/gamemesh/internal/simulator"
)

func TestRunAllocatesRequestedPlayersWhenCapacityIsEnough(t *testing.T) {
	cluster := simulator.NewHeterogeneousCluster(10, 100)
	result := Run(context.Background(), Config{Players: 500, Workers: 4, Version: "v1"}, cluster, scheduler.NewRoundRobin())
	if result.SuccessfulAllocations != 500 {
		t.Fatalf("successful=%d want 500; failed=%d", result.SuccessfulAllocations, result.FailedAllocations)
	}
	if result.FailedAllocations != 0 {
		t.Fatalf("failed=%d want 0", result.FailedAllocations)
	}
	if result.ThroughputPerSecond <= 0 {
		t.Fatalf("throughput must be > 0")
	}
}

func TestRunWithRegistrySourceAllocatesRequestedPlayers(t *testing.T) {
	cluster := simulator.NewHeterogeneousCluster(10, 100)
	source, err := NewRegistryCandidateSource(cluster, registry.New(registry.Config{}), 10)
	if err != nil {
		t.Fatal(err)
	}
	result, err := RunWithSource(context.Background(), Config{Players: 500, Workers: 1, Version: "v1"}, cluster, scheduler.NewRoundRobin(), source)
	if err != nil {
		t.Fatal(err)
	}
	if result.CandidateSource != "registry_snapshot" || result.SuccessfulAllocations != 500 || result.FailedAllocations != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	var registeredPlayers int
	view := source.registry.Snapshot()
	for i := 0; i < view.Len(); i++ {
		registeredPlayers += view.At(i).CurrentPlayers
	}
	if registeredPlayers != 500 {
		t.Fatalf("registry players=%d want 500", registeredPlayers)
	}
}
