package main

import (
	"context"
	"fmt"
	"time"

	"github.com/gamemesh-labs/gamemesh/internal/registry"
	"github.com/gamemesh-labs/gamemesh/internal/scheduler"
	"github.com/gamemesh-labs/gamemesh/internal/simulator"
	"github.com/gamemesh-labs/gamemesh/pkg/model"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reg := registry.New(registry.Config{
		HeartbeatTTL:  250 * time.Millisecond,
		SweepInterval: 50 * time.Millisecond,
	})
	go func() { _ = reg.Run(ctx) }()

	servers := []*simulator.GameServer{
		simulator.NewGameServer(simulator.ServerConfig{ID: "gs-a", Address: "127.0.0.1:7001", Region: "sg", Zone: "a", Capacity: 1000}),
		simulator.NewGameServer(simulator.ServerConfig{ID: "gs-b", Address: "127.0.0.1:7002", Region: "sg", Zone: "b", Capacity: 1000}),
		simulator.NewGameServer(simulator.ServerConfig{ID: "gs-c", Address: "127.0.0.1:7003", Region: "sg", Zone: "c", Capacity: 1000}),
	}

	hbCancels := make([]context.CancelFunc, 0, len(servers))
	for _, server := range servers {
		hbCtx, hbCancel := context.WithCancel(ctx)
		hbCancels = append(hbCancels, hbCancel)
		go func(s *simulator.GameServer) { _ = registry.RunHeartbeat(hbCtx, reg, s, 50*time.Millisecond) }(server)
	}

	time.Sleep(100 * time.Millisecond)
	printView("initial", reg)

	// Simulate gs-c crashing: its heartbeat disappears without deregistration.
	hbCancels[2]()
	time.Sleep(350 * time.Millisecond)
	printView("after gs-c heartbeat timeout", reg)

	rr := scheduler.NewRoundRobin()
	source := registry.CandidateSource{Registry: reg}
	for i := 0; i < 4; i++ {
		result, err := rr.Schedule(ctx, model.AllocationRequest{Region: "sg"}, source.Candidates())
		if err != nil {
			panic(err)
		}
		fmt.Printf("schedule %d -> %s\n", i+1, result.GameServerID)
	}
}

func printView(label string, reg *registry.Registry) {
	view := reg.Snapshot()
	fmt.Printf("\n[%s] generation=%d\n", label, view.Generation())
	for i := 0; i < view.Len(); i++ {
		server := view.At(i)
		fmt.Printf("%-4s %-10s %-12s %s\n", server.ID, server.Region, server.State, server.Address)
	}
}
