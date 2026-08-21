# GameMesh

GameMesh 是一个面向多人在线游戏的自适应流量与 GameServer 调度层。

当前版本：**M1 / Registry & Failure Detection**。

## M1 已实现

在 M0 的领域模型、Simulator、RoundRobin 和 Benchmark 基础上新增：

- GameServer Registry；
- GameServer 地址 / Zone 服务发现字段；
- Heartbeat Lease；
- TTL Failure Detection；
- `UNHEALTHY` 自动隔离与心跳恢复；
- Graceful Deregister；
- 原子发布的只读 Registry View；
- Heartbeat O(1) 更新 + 周期批量快照发布；
- 节点状态变化立即发布；
- Registry → Scheduler 集成测试；
- 并发 / Race Test；
- Registry 微基准。

## 核心链路

```text
GameServer / Simulator
        │
        │ Register + Heartbeat
        ▼
┌─────────────────────────┐
│ GameServer Registry     │
│                         │
│ Lease / TTL             │
│ Failure Detection       │
│ Membership              │
│ Metrics                 │
└───────────┬─────────────┘
            │ batched publish
            ▼
     Immutable View
            │ O(1) read
            ▼
        Scheduler
            │
            ▼
       GameServer
```

普通指标心跳只更新 Registry Entry 并标记 dirty；`PublishInterval` 到期后批量生成一个新 View。`READY -> DRAINING`、超时成为 `UNHEALTHY`、恢复等会影响路由正确性的状态变化立即发布。

## 目录

```text
GameMesh-M1/
├── cmd/
│   ├── benchmark/          # M0 基准实验
│   ├── registry-demo/      # M1 注册/心跳/故障演示
│   └── simulator/
├── internal/
│   ├── benchmark/
│   ├── registry/           # M1 新增
│   ├── scheduler/
│   └── simulator/
├── pkg/model/
├── benchmarks/results/
├── docs/
│   ├── foundation/
│   ├── 06_m0_implementation.md
│   ├── 07_m0_baseline_report.md
│   ├── 08_m1_implementation.md
│   └── 09_m1_validation_report.md
└── tests/
```

## 运行

需要 Go 1.23+。

```bash
go test ./...
go vet ./...
go test -race ./...
```

运行 M1 Failure Detection 演示：

```bash
go run ./cmd/registry-demo
```

运行原 M0 Benchmark：

```bash
go run ./cmd/benchmark -servers 100 -capacity 1000 -players 10000 -workers 8
```

运行 Registry 微基准：

```bash
go test ./internal/registry -run '^$' -bench . -benchmem
go test ./internal/simulator -run '^$' -bench BenchmarkClusterSnapshots1000Servers -benchmem
```

## 当前边界

M1 的 Registry 仍是单进程 In-Memory Control Plane，不提供跨节点一致性；这符合当前里程碑目标。后续接 Kubernetes/Agones 或 etcd 时，可以通过 Adapter 替换持久化/发现层，而不改变 Scheduler 的领域模型。

M1 解决了请求路径重复构造 Cluster Snapshot 的问题，但 M0 RoundRobin 仍会每次遍历全部候选并创建 `eligible` 切片。**M2 将以 Scheduler 数据结构与负载策略为核心解决这一瓶颈。**

下一阶段：**M2 Load Balancer：LeastConnection / Weighted / LeastLoad / LoadScore + RoundRobin 对照实验。**
