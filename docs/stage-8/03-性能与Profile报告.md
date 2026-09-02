# Stage 8：性能与 Profile 报告

## 环境

```text
Date: 2026-08-31
Go: go1.25.4 linux/amd64
OS: Linux 6.18.33.2-microsoft-standard-WSL2
Logical CPUs: 8
Memory available: 13 GiB
Network: httptest + loopback TCP
```

## 稳态消息基线

命令：

```bash
go run ./cmd/bench -clients 20 -messages 100 -payload 256
```

| 指标 | 结果 |
| --- | ---: |
| 成功消息 / 失败 | 2000 / 0 |
| 吞吐 | 112,410.74 msg/s |
| P50 / P95 / P99 | 106.3µs / 229.1µs / 1.590799ms |
| 完成时 Alloc / Sys | 898,552 B / 18,700,552 B |
| 完成时 goroutine | 5 |

这是单进程 loopback Echo 基线，未包含 TLS、真实认证、Redis 往返或业务 Backend 的网络成本。Stage 7 的全局限流默认 20,000 msg/s，若压测要研究更高吞吐，必须显式提高该参数并记录它，不能误将保护阈值当成能力上限。

## Profile

命令：

```bash
go test ./internal/gateway -run TestMultipleConcurrentClients -count=20 \
  -cpuprofile /tmp/gamemesh-stage8-cpu.pprof \
  -memprofile /tmp/gamemesh-stage8-heap.pprof
go tool pprof -top /tmp/gamemesh-stage8-cpu.pprof
go tool pprof -top /tmp/gamemesh-stage8-heap.pprof
```

CPU 样本主要在 loopback syscall、WebSocket frame 读写；不是 Gateway 的游戏逻辑热点。alloc_space 主要来自 Envelope marshal 与每次 WebSocket 连接的 bufio reader/writer 分配。当前阶段不据此做微优化：先保留正确性与有界队列，后续应在真实 State Sync 流量和目标机器上复测。

## 可复现验证

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
```
