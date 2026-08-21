package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"

	bench "github.com/gamemesh-labs/gamemesh/internal/benchmark"
	"github.com/gamemesh-labs/gamemesh/internal/scheduler"
	"github.com/gamemesh-labs/gamemesh/internal/simulator"
)

func main() {
	players := flag.Int("players", 10000, "number of simulated player allocations")
	servers := flag.Int("servers", 100, "number of simulated game servers")
	capacity := flag.Int("capacity", 1000, "base capacity per game server")
	workers := flag.Int("workers", runtime.GOMAXPROCS(0)*2, "allocation worker count")
	maxRetries := flag.Int("max-retries", 8, "retries for optimistic allocation races")
	region := flag.String("region", "", "optional region filter")
	version := flag.String("version", "v1", "optional game version filter")
	out := flag.String("out", "", "optional JSON output path")
	flag.Parse()

	cluster := simulator.NewHeterogeneousCluster(*servers, *capacity)
	strategy := scheduler.NewRoundRobin()
	result := bench.Run(context.Background(), bench.Config{
		Players: *players, Workers: *workers, Region: *region, Version: *version, MaxRetries: *maxRetries,
	}, cluster, strategy)

	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(encoded))
	if *out != "" {
		if err := bench.WriteJSON(*out, result); err != nil {
			fmt.Fprintln(os.Stderr, "write result:", err)
			os.Exit(1)
		}
	}
}
