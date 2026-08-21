# GameMesh

GameMesh 是一个面向多人在线游戏的自适应流量与 GameServer 调度层。

当前版本：**M0 / Baseline**。

## M0 已实现

- 核心领域模型；
- GameServer Simulator；
- 异构 GameServer Cluster；
- Scheduler 插件接口；
- RoundRobin 基准调度器；
- 并发 Benchmark Framework；
- 基础单元测试。

## 目录

```text
GameMesh-M0/
├── cmd/
│   ├── benchmark/       # 基准实验 CLI
│   └── simulator/       # Simulator 快速预览
├── internal/
│   ├── benchmark/
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

下一阶段：**M1 GameServer Registry + Heartbeat + Failure Detection**。
