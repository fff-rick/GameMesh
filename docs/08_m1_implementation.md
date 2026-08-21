# GameMesh M1 Implementation

## 1. 目标

M1 的目标不是做分布式注册中心，而是建立 GameMesh 后续所有调度策略都能依赖的“节点事实层”：

1. GameServer 能注册自身；
2. GameServer 通过 Heartbeat 续租；
3. 心跳中断后 Registry 能自动判定节点不可用；
4. Scheduler 不应读取失效节点；
5. Scheduler 获取候选集合时不能每个请求都重新扫描、加锁和复制整个模拟集群；
6. 后续可替换为 Kubernetes/Agones/etcd Adapter，而不破坏领域模型。

## 2. 新增组件

### `internal/registry/InMemory`

维护：

- `Entry.Server`：GameServer 最新状态与指标；
- `RegisteredAt`；
- `LastHeartbeat`；
- `ExpiresAt`；
- `Generation`；
- Registry `Revision`；
- 原子发布的 `View`。

### Heartbeat Lease

```text
Register
   │
   ▼
READY
   │ heartbeat
   ├───────────────┐
   ▼               │
ExpiresAt 延后      │
                   │
心跳停止            │
   ▼               │
TTL 到期            │
   ▼               │
UNHEALTHY ◄────────┘ heartbeat recovery
```

Crash 不执行 Deregister，由 TTL 检测；Graceful Shutdown 才主动 Deregister。

## 3. 快照发布模型

M0 中每次 Schedule 都调用：

```text
cluster.Snapshots()
```

该操作需要遍历 GameServer、获取锁并重新构造切片，复杂度接近 O(N)。

M1 改为：

```text
Heartbeat ──► Mutable Registry Entries
                    │
                    │ PublishInterval
                    ▼
              Immutable View
                    │
                    │ atomic load
                    ▼
                Scheduler
```

### 为什么不在每次 Heartbeat 后发布？

第一版实现曾这样做。微基准证明，对 1000 个节点，每个心跳都复制/发布整个节点池会把 O(N) 成本转移到 Heartbeat 热路径。

因此最终 M1 策略为：

- 普通 Metrics / CurrentPlayers 心跳：O(1) 更新 Entry，标记 `dirty`；
- 周期到达：一次批量 Publish；
- 状态变化：立即 Publish；
- TTL 超时：立即 Publish；
- Register / Deregister：立即 Publish。

它在“指标新鲜度”和“控制面写放大”之间做了明确取舍。

## 4. Failure Detection

`ExpireStale(now)` 根据 `ExpiresAt` 判断租约是否过期。

超时节点不会立刻删除，而是：

```text
READY / ALLOCATED
        │
        ▼
    UNHEALTHY
```

这样保留节点身份与诊断信息，并允许后续 Heartbeat 恢复。

`TERMINATED` 不会被 TTL 逻辑重写。

## 5. 服务发现字段

M1 为 `GameServerSnapshot` 增加：

- `Address`；
- `Zone`。

至此 Registry View 已能表达：

```text
GameServerID -> Address + Region + Zone + Version + State + Capacity + Metrics
```

后续 Gateway 可以根据 Scheduler 返回的 ID 查询/缓存实际后端地址。

## 6. 并发模型

- Mutation：单 Registry Mutex；
- Published View：`atomic.Pointer[View]`；
- Scheduler read：无 Registry 锁；
- View 中只包含值类型字段，不含可变 Map/Slice 子字段；
- Scheduler contract 明确禁止修改候选快照。

当前单锁设计适合 M1 单进程控制面。大规模多控制器写入、分片 Registry、跨节点一致性不属于本阶段范围。

## 7. M1 不做什么

- 不实现 etcd/Raft；
- 不接 Kubernetes/Agones；
- 不做主动 TCP 健康探测；
- 不实现 LeastLoad；
- 不实现真实 Gateway；
- 不实现运行中 Battle 迁移。

主动探测暂缓的原因：仅能连通端口不代表 GameServer 可接受新 Match；GameServer 自报 Heartbeat + 业务指标更适合作为当前阶段的 Ready/Load 信号。

## 8. 暴露出的 M2 问题

Registry View 获取已经是 O(1)，但 M0 `RoundRobin.Schedule()` 仍然：

1. 遍历完整候选集；
2. 根据 State/Region/Version 过滤；
3. 构造新的 `eligible` Slice。

因此 M2 不仅要增加 LeastConnection / LeastLoad，还需要考虑：

- Region/Version 预索引；
- Candidate Pool；
- 避免每请求分配临时 Slice；
- Strategy 间统一 Benchmark。
