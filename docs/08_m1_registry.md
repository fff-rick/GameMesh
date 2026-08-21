# M1：GameServer Registry、Heartbeat 与 Failure Detection

## 决策

M1 使用单进程内存 Registry，作为 Control Plane 对 GameServer 成员关系、近实时
负载与健康状态的权威视图。每次注册、心跳、失效或注销都会构造并原子发布新的
不可变 Snapshot；Scheduler 只读取 Snapshot，不持有 Registry 锁，也不能修改已发布
元素。

```text
GameServer -- Register / Heartbeat --> Registry writer lock
                                         |
                                         | publish a new immutable Snapshot
                                         v
Scheduler ------------------------> atomic Snapshot load (O(1))
```

这是有条件推荐的实现：它适合当前单进程 M1 和读多写少的控制面。它不试图替代
Kubernetes/Agones，也不承诺多个 GameMesh 副本之间的强一致性。把 Redis、etcd
watch、SWIM 或 gossip 放进现在的 M1 会增加故障模型和运维成本，却没有真实的
多副本需求来证明收益。M5 的 Adapter 将把 Agones/Kubernetes 生命周期映射进
Registry；届时再按部署拓扑选择外部状态源。

## 状态与失败语义

- `Register` 创建或刷新成员的静态调度属性：ID、Region、Version、Capacity。
- `Heartbeat` 只能更新已注册成员的动态属性：State、玩家数和指标；未知 ID 被拒绝。
- 每次成功 Heartbeat 都刷新 TTL。
- `Sweep` 检测到 TTL 到期时，将非终止成员标记为 `UNHEALTHY`，不直接删除它，
  以保留故障可观测性。
- `UNHEALTHY` 不符合 `CanAccept`，因而立即从新调度候选中剔除；后续成功
  Heartbeat 可使节点按报告状态恢复。
- 已确认永久终止时调用 `Deregister`，从 Registry 删除成员。

默认 TTL 为 3 秒、扫描间隔为 TTL/3。它们是开发期默认值，不是生产 SLA；生产值应
结合 Heartbeat 周期、网络抖动预算和“节点 Unhealthy 后 < 3 秒停止接收新 Match”
的验收目标校准。

## 并发与性能边界

Registry 写路径由互斥锁串行化。发布使用 `atomic.Pointer`；读路径无需锁、无需复制
完整 Snapshot。Snapshot 用 `Len`/`At` 返回值访问，不导出内部切片，防止 Scheduler
意外修改共享状态。测试同时覆盖快照版本隔离、TTL 剔除、输入校验和并发读写，并以
`go test -race` 验证。

这只消除了“获取 Cluster Snapshot”的每次请求 O(N) 开销。M0 RoundRobin 为维持
Region/Version/Capacity 过滤仍会遍历候选集，且当前算法有两次无分配扫描；这项成本
必须在 M2 通过 P2C、按 Region/Version 的预过滤索引或其他经基准验证的策略解决。
不要在 M1 提前实现 LeastLoad，否则会失去 RoundRobin 与后续策略之间的可解释对照组。
