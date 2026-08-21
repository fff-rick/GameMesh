# 06. M0 实现说明

## 目标

M0 只建立可验证的核心基线：领域模型、GameServer Simulator、RoundRobin Scheduler 和 Benchmark Framework。

## 已实现

- Player / Party / Match / GameServer / AllocationRequest 数据模型；
- GameServer 生命周期状态；
- 异构 GameServer Cluster；
- 容量、CPU、Memory、Network、TickLatency、Queue 指标模拟；
- 故障/Draining 状态注入接口；
- Scheduler 抽象；
- RoundRobin Baseline；
- Region / Version 基础候选过滤；
- 并发 Allocation Benchmark；
- P50 / P95 / P99、吞吐、失败数、Utilization StdDev、Overloaded Server 等指标；
- 单元测试。

## 设计约束

1. M0 不引入 Redis、Kafka、Kubernetes、Agones。
2. Simulator 是确定性近似模型，不代表真实游戏服务器性能。
3. RoundRobin 不使用 CPU/Tick/LoadScore，以保证它能作为后续策略的公平基准。
4. Match Live Migration 暂不支持。
5. M0 的性能数据只作为代码回归与策略对照，不写入简历作为生产性能。

## 下一里程碑

M1 建议实现：

- GameServer Registry；
- Heartbeat / TTL；
- Healthy / Unhealthy 收敛；
- Service Discovery Snapshot；
- Scheduler 与 Registry 解耦；
- 节点故障实验。
