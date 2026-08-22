package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"

	bench "github.com/gamemesh-labs/gamemesh/internal/benchmark"
	"github.com/gamemesh-labs/gamemesh/internal/registry"
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
	sourceMode := flag.String("candidate-source", "cluster", "candidate source: cluster or registry")
	registryPublishEvery := flag.Int("registry-publish-every", 100, "successful allocations between Registry metric publications")
	flag.Parse()

	cluster := simulator.NewHeterogeneousCluster(*servers, *capacity)
	strategy := scheduler.NewRoundRobin()
	config := bench.Config{
		Players: *players, Workers: *workers, Region: *region, Version: *version, MaxRetries: *maxRetries,
	}
	var source bench.CandidateSource
	switch *sourceMode {
	case "cluster":
		source = bench.NewClusterCandidateSource(cluster)
	case "registry":
		var err error
		source, err = bench.NewRegistryCandidateSource(cluster, registry.New(registry.Config{}), *registryPublishEvery)
		if err != nil {
			fmt.Fprintln(os.Stderr, "create registry candidate source:", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "candidate-source must be cluster or registry")
		os.Exit(2)
	}
	result, err := bench.RunWithSource(context.Background(), config, cluster, strategy, source)
	if err != nil {
		fmt.Fprintln(os.Stderr, "run benchmark:", err)
		os.Exit(1)
	}

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
