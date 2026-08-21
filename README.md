# GameMesh

GameMesh 是一个面向多人在线游戏的自适应流量与 GameServer 调度层。

当前版本：**M1 / GameServer Registry**（M0 Baseline 已保留）。

## M0 已实现

- 核心领域模型；
- GameServer Simulator；
- 异构 GameServer Cluster；
- Scheduler 插件接口；
- RoundRobin 基准调度器；
- 并发 Benchmark Framework；
- 基础单元测试。

## M1 已实现

- Control Plane 内存 GameServer Registry；
- 显式注册与受校验的 Heartbeat；
- 可配置 TTL Failure Detection，超时节点转为 `UNHEALTHY`；
- `Deregister` 用于已确认终止节点；
- 按 ID 稳定排序、原子发布的只读 Snapshot；
- Registry 写入与 Scheduler 读取之间的并发隔离。

## 目录

```text
GameMesh/
├── cmd/
│   ├── benchmark/       # 基准实验 CLI
│   └── simulator/       # Simulator 快速预览
├── internal/
│   ├── benchmark/
│   ├── registry/       # M1 membership / heartbeat / failure detection
│   ├── scheduler/
│   └── simulator/
├── pkg/
│   └── model/           # 可供未来 adapter/gateway 复用的领域模型
├── tests/               # M1+ 跨模块测试预留
├── benchmarks/
│   └── results/
└── docs/
    ├── foundation/      # v0.1 立项与可行性调研
    └── 06_m0_implementation.md
```

## 运行

需要 Go 1.23+。

```bash
go test ./...
go run ./cmd/simulator
go run ./cmd/benchmark -servers 100 -capacity 1000 -players 10000 -workers 8
```

保存基准结果：

```bash
go run ./cmd/benchmark \
  -servers 100 \
  -capacity 1000 \
  -players 100000 \
  -workers 8 \
  -out benchmarks/results/round-robin-100k.json
```

## 当前边界

M0 不包含真实网络 Gateway、Redis/Kafka、Kubernetes/Agones、自动扩缩容或智能 LoadScore。它的职责是提供后续所有策略可公平比较的 Baseline。

## M1 边界

M1 的 Registry 是单进程、内存中的 Control Plane 权威视图；它还不是
跨副本一致的服务发现系统，也不包含 Gateway、TCP/WebSocket、Redis、Kubernetes
或 Agones Adapter。未来 Adapter 负责将外部编排状态映射到这个领域模型，避免
污染 Scheduler 热路径。

`Registry.Snapshot()` 是 O(1) 的原子读取，避免 M0 每次调度都从 Cluster
重新构建完整 Snapshot。RoundRobin 为了维持它作为基线策略的语义，仍会遍历
候选节点（但不分配临时候选切片）；M2 应以 P2C / 预过滤候选索引解决调度算法
本身的 O(N) 扫描，而不能误把 Registry 的缓存当成最终热路径优化。

下一阶段：**M2 Least Connection / P2C / LoadScore 对照实验**。
