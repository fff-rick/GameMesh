# Benchmarks

M0 provides an executable baseline benchmark rather than claiming production performance.

Example:

```bash
go run ./cmd/benchmark -servers 100 -capacity 1000 -players 100000 -workers 8 -out benchmarks/results/round-robin-100k.json
```

Metrics include scheduler latency P50/P95/P99, allocation throughput, success/failure counts, utilization standard deviation, max utilization, overloaded server count, and per-server distribution.

The numbers are simulator results only. They must not be presented as real network/GameServer capacity data.
