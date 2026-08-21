package benchmark

import (
	"context"
	"testing"

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
