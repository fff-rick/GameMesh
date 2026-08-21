package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gamemesh-labs/gamemesh/internal/scheduler"
	"github.com/gamemesh-labs/gamemesh/internal/simulator"
	"github.com/gamemesh-labs/gamemesh/pkg/model"
)

type Config struct {
	Players    int
	Workers    int
	Region     string
	Version    string
	MaxRetries int
}

type ServerDistribution struct {
	ID          string  `json:"id"`
	Players     int     `json:"players"`
	Capacity    int     `json:"capacity"`
	Utilization float64 `json:"utilization"`
	CPUPercent  float64 `json:"cpu_percent"`
	TickMillis  float64 `json:"tick_ms"`
}

type Result struct {
	Strategy              string               `json:"strategy"`
	PlayersRequested      int                  `json:"players_requested"`
	SuccessfulAllocations int64                `json:"successful_allocations"`
	FailedAllocations     int64                `json:"failed_allocations"`
	AllocationRetries     int64                `json:"allocation_retries"`
	DurationMillis        float64              `json:"duration_ms"`
	ThroughputPerSecond   float64              `json:"throughput_per_second"`
	ScheduleP50Micros     float64              `json:"schedule_p50_us"`
	ScheduleP95Micros     float64              `json:"schedule_p95_us"`
	ScheduleP99Micros     float64              `json:"schedule_p99_us"`
	AverageUtilization    float64              `json:"average_utilization"`
	UtilizationStdDev     float64              `json:"utilization_stddev"`
	MaxUtilization        float64              `json:"max_utilization"`
	OverloadedServers     int                  `json:"overloaded_servers"`
	ServerDistribution    []ServerDistribution `json:"server_distribution"`
}

func Run(ctx context.Context, cfg Config, cluster *simulator.Cluster, strategy scheduler.Scheduler) Result {
	if cfg.Players <= 0 {
		cfg.Players = 10000
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 1
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 8
	}

	jobs := make(chan int, cfg.Workers*2)
	latencies := make([]time.Duration, cfg.Players)
	var successful atomic.Int64
	var failed atomic.Int64
	var retries atomic.Int64
	var wg sync.WaitGroup

	started := time.Now()
	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				select {
				case <-ctx.Done():
					failed.Add(1)
					continue
				default:
				}

				req := model.AllocationRequest{
					PlayerID: fmt.Sprintf("player-%d", i),
					Region:   cfg.Region,
					Version:  cfg.Version,
				}
				before := time.Now()
				allocated := false
				for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
					selection, err := strategy.Schedule(ctx, req, cluster.Snapshots())
					if err != nil {
						break
					}
					if err := cluster.Allocate(selection.GameServerID, 1); err != nil {
						if attempt < cfg.MaxRetries {
							retries.Add(1)
							continue
						}
						break
					}
					allocated = true
					break
				}
				latencies[i] = time.Since(before)
				if allocated {
					successful.Add(1)
				} else {
					failed.Add(1)
				}
			}
		}()
	}
	for i := 0; i < cfg.Players; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	duration := time.Since(started)

	return buildResult(strategy.Name(), cfg.Players, successful.Load(), failed.Load(), retries.Load(), duration, latencies, cluster.Snapshots())
}

func buildResult(strategy string, requested int, successful, failed, retries int64, duration time.Duration, latencies []time.Duration, snapshots []model.GameServerSnapshot) Result {
	validLatencies := make([]time.Duration, 0, len(latencies))
	for _, d := range latencies {
		if d > 0 {
			validLatencies = append(validLatencies, d)
		}
	}
	sort.Slice(validLatencies, func(i, j int) bool { return validLatencies[i] < validLatencies[j] })

	dist := make([]ServerDistribution, 0, len(snapshots))
	utils := make([]float64, 0, len(snapshots))
	overloaded := 0
	maxUtil := 0.0
	for _, s := range snapshots {
		u := s.Utilization()
		utils = append(utils, u)
		if u >= 0.90 {
			overloaded++
		}
		if u > maxUtil {
			maxUtil = u
		}
		dist = append(dist, ServerDistribution{
			ID: s.ID, Players: s.CurrentPlayers, Capacity: s.Capacity,
			Utilization: u, CPUPercent: s.Metrics.CPUPercent, TickMillis: s.Metrics.TickLatencyMillis,
		})
	}

	avg := mean(utils)
	stddev := stddev(utils, avg)
	throughput := 0.0
	if duration > 0 {
		throughput = float64(successful+failed) / duration.Seconds()
	}

	return Result{
		Strategy: strategy, PlayersRequested: requested,
		SuccessfulAllocations: successful, FailedAllocations: failed, AllocationRetries: retries,
		DurationMillis: duration.Seconds() * 1000, ThroughputPerSecond: throughput,
		ScheduleP50Micros:  percentileMicros(validLatencies, 0.50),
		ScheduleP95Micros:  percentileMicros(validLatencies, 0.95),
		ScheduleP99Micros:  percentileMicros(validLatencies, 0.99),
		AverageUtilization: avg, UtilizationStdDev: stddev,
		MaxUtilization: maxUtil, OverloadedServers: overloaded,
		ServerDistribution: dist,
	}
}

func percentileMicros(values []time.Duration, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	idx := int(math.Ceil(float64(len(values))*p)) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(values) {
		idx = len(values) - 1
	}
	return float64(values[idx].Nanoseconds()) / 1000
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var total float64
	for _, v := range values {
		total += v
	}
	return total / float64(len(values))
}

func stddev(values []float64, avg float64) float64 {
	if len(values) == 0 {
		return 0
	}
	var sum float64
	for _, v := range values {
		d := v - avg
		sum += d * d
	}
	return math.Sqrt(sum / float64(len(values)))
}

func WriteJSON(path string, result Result) error {
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o644)
}
