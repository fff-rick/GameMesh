# GameMesh

GameMesh 是一个面向多人在线游戏的自适应流量与 GameServer 调度层。

当前版本：**M1 / GameServer Registry**（M0 Baseline 已保留）。

## M1 已实现

- 单进程内存 Registry：注册、Heartbeat、优雅注销；
- `Address` / `Zone` 服务发现字段；
- TTL Failure Detection：超时节点转为 `UNHEALTHY` 并从调度候选剔除；
- 原子发布的不可变 Snapshot：Scheduler 通过 `Len` / `At` 按值读取；
- Heartbeat 仅更新状态、人数和指标，不能改写容量、Region、Version 等静态调度约束；
- 状态变化、注册、注销和 TTL 失效立即发布；非关键负载指标按 `PublishInterval` 批量发布；
- Registry → Scheduler 集成测试、并发/Race 测试、Failure Detection Demo 和微基准。

## 核心链路

```text
GameServer / Simulator
        │ Register + Heartbeat
        ▼
Registry ── lease / TTL ──► immutable Snapshot ──► Scheduler
```

## 运行

需要 Go 1.23+。

```bash
go test ./...
go vet ./...
go test -race ./...
go run ./cmd/registry-demo
go run ./cmd/benchmark -servers 100 -capacity 1000 -players 10000 -workers 8 -candidate-source cluster
go run ./cmd/benchmark -servers 100 -capacity 1000 -players 10000 -workers 8 -candidate-source registry -registry-publish-every 100
go test ./internal/registry -run '^$' -bench . -benchmem
```

## 当前边界

Registry 是单进程内存中的 Control Plane 权威视图，不提供跨副本一致性，也未包含 Gateway、TCP/WebSocket、Redis、Kubernetes 或 Agones Adapter。Benchmark 可切换 Simulator Snapshot 与 Registry Snapshot；`registry-publish-every` 明确控制基准中的负载视图陈旧度，不能将单次结果视为线上 SLA。

RoundRobin 仍须遍历候选集；M2 应以 P2C、Least Connection / LoadScore 与预过滤索引做对照，而不是将 Registry 快照缓存误认为完整的调度性能优化。
