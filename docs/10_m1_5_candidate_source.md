# M1.5：Registry Candidate Source 对照实验

## 决策

Benchmark 支持两条候选来源路径：

```text
cluster  : Simulator Cluster.Snapshots() -> Scheduler
registry : Simulator allocation -> Heartbeat -> Registry Snapshot -> Scheduler
```

`cluster` 保留 M0 行为，作为对照组。`registry` 在初始化时注册全部
Simulator 节点；每次成功分配后，只将被选节点的最新动态信息写入
Heartbeat。静态调度属性仍只能由 Register 修改。

Registry 的负载更新按 `-registry-publish-every` 个成功分配批量发布；
状态转换仍由 Registry 立即发布。该参数只用于控制可重复实验中的快照
陈旧度，不代表生产环境的 Heartbeat 周期或 SLA。

## 运行

```bash
go run ./cmd/benchmark -servers 100 -capacity 1000 -players 10000 -workers 8 \
  -candidate-source cluster

go run ./cmd/benchmark -servers 100 -capacity 1000 -players 10000 -workers 8 \
  -candidate-source registry -registry-publish-every 100
```

输出的 `candidate_source` 字段必须分别为 `cluster_snapshot` 和
`registry_snapshot`。比较时应同时记录成功/失败/重试、分配分布和 P50/P95/P99；
不要仅凭一次吞吐结果做性能结论。

## 验收

- 两条路径在容量充足、单 worker 的确定性场景下有相同的成功/失败结果；
- Registry 路径在最终 Flush 后反映 Simulator 的玩家总数；
- 原有 Registry 故障隔离测试仍通过；
- `go test -race ./...` 通过。

## 非目标

本阶段不实现候选预索引，也不承诺 Scheduler 为 O(1)。RoundRobin 仍需遍历候选；
M2 将在此可比较实验基础上加入 P2C 并评估是否值得维护更多索引。
