package main

import (
	"fmt"

	"github.com/gamemesh-labs/gamemesh/internal/simulator"
)

func main() {
	cluster := simulator.NewHeterogeneousCluster(6, 1000)
	fmt.Println("GameMesh M0 GameServer Simulator")
	for _, server := range cluster.Snapshots() {
		fmt.Printf("%s region=%s capacity=%d state=%s cpu=%.1f%% tick=%.1fms\n",
			server.ID, server.Region, server.Capacity, server.State,
			server.Metrics.CPUPercent, server.Metrics.TickLatencyMillis)
	}
}
