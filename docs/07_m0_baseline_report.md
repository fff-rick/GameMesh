# 07. M0 Baseline Benchmark Report

## 1. 目的

本报告验证 M0 基线代码可以稳定运行，并记录 RoundRobin 在**异构 GameServer 池**中的第一组对照数据。该结果来自本地模拟器，不代表真实网络、真实游戏进程或生产服务器性能。

## 2. 场景

- Scheduler：`RoundRobin`
- GameServer：100
- Base Capacity：1000 players/server
- 异构容量系数：0.8 / 1.0 / 1.2 / 1.4 循环
- Allocation：100,000 players
- Worker：8
- Version Filter：v1
- 网络：未启用；纯进程内模拟

## 3. 结果

| 指标 | 结果 |
|---|---:|
| 请求玩家数 | 100,000 |
| 成功分配 | 100,000 |
| 失败分配 | 0 |
| 乐观并发重试 | 1 |
| 总耗时 | 842.22 ms |
| 模拟 Allocation Throughput | 118,734/s |
| Schedule P50 | 11.85 μs |
| Schedule P95 | 214.90 μs |
| Schedule P99 | 741.81 μs |
| 平均容量利用率 | 92.56% |
| Utilization StdDev | 8.76 percentage points |
| 最大容量利用率 | 100.00% |
| >=90% 利用率节点 | 75 / 100 |

## 4. 第一阶段观察

RoundRobin 对“节点数量”公平，但对“异构容量”不公平。相同玩家数被轮询到不同容量的服务器后，小容量节点更早达到高利用率。当前 100k 场景中已经出现大量 >=90% 的节点，因此后续 LeastLoad / LoadScore 可以用以下指标直接与基线比较：

1. `UtilizationStdDev` 是否下降；
2. `OverloadedServers` 是否减少；
3. `MaxUtilization` 是否降低；
4. 在不明显恶化 P99 的情况下，是否提高可用容量利用效率。

## 5. 已发现的工程问题

M0 每次调度都会重新构建整个 Cluster Snapshot，复杂度约为 O(N)。这在模拟阶段有利于保证数据新鲜，但不应该直接成为最终 Data Plane 实现。M1 Registry 应引入只读快照/版本化视图，把“状态更新”和“热路径读取”解耦。

并发 Worker 在节点接近满载时可能同时观察到剩余容量，产生乐观分配竞争。Benchmark 已记录并重试这种情况；本次出现 1 次重试、0 次最终失败。后续 Registry/Allocator 需要定义正式的容量预留或原子 Allocation 语义。

## 6. 结论

M0 的目标已经成立：我们现在拥有一套可以重复运行的 Baseline。M1 开始后，不应急着增加更多 Balancer，而应该先解决 Registry、Heartbeat、Snapshot 一致性和节点故障收敛，因为这些能力决定后续所有调度结果是否可信。
