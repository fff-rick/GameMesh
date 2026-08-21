# GameMesh M1 Validation Report

## 1. 验证结论

M1 当前通过：

```text
go test ./...
go vet ./...
go test -race ./...
```

验证覆盖：

- 注册顺序与 Snapshot 发布；
- 未注册节点 Heartbeat 拒绝；
- TTL 超时 -> UNHEALTHY；
- Heartbeat 恢复；
- TERMINATED 状态保护；
- Deregister；
- Metrics Heartbeat 批量发布；
- Registry + Scheduler 故障隔离；
- 并发 Heartbeat / Snapshot Read；
- Race Detector。

## 2. Failure Detection Demo

运行：

```bash
go run ./cmd/registry-demo
```

实验包含 `gs-a / gs-b / gs-c` 三个节点。停止 `gs-c` Heartbeat 后，TTL 到期：

```text
gs-a READY
gs-b READY
gs-c UNHEALTHY
```

RoundRobin 后续调度只返回：

```text
gs-a
gs-b
gs-a
gs-b
```

说明 Failure Detection 已进入实际 Scheduler 候选链路，而不是孤立功能。

## 3. Registry 微基准

测试机器：容器环境，Intel Xeon Platinum 8370C；数字仅用于当前实现的相对对比，不能作为线上 SLA。

### 1000 GameServer：Published View Read

约：

```text
0.85 - 0.92 ns/op
0 B/op
0 allocs/op
```

该微基准只测 `atomic.Pointer` 加载，不包含调度策略执行。

### 普通 Heartbeat Entry Update

约：

```text
173 - 181 ns/op
0 B/op
0 allocs/op
```

普通 Metrics Heartbeat 不再重建整个 View。

### 1000 GameServer：一次批量 Publish

在缓存稳定 Membership 顺序后约：

```text
90 - 108 µs/op
~155 KB/op
2 allocs/op
```

第一版“每 Heartbeat Publish”曾达到约 300+ µs/次，因此 M1 已改成定期批量发布。

### M0 Cluster Snapshot Rebuild 对照

1000 GameServer 重新遍历并复制 Cluster Snapshot 约：

```text
161 - 171 µs/op
~180 KB/op
3 allocs/op
```

两者不能简单理解成“Registry 快 N 倍”：Registry 把复制成本从每个请求移动到低频 Control Plane Publish，而 Scheduler 读取的是已发布 View。真正价值是**改变成本发生的位置与频率**。

## 4. M0 回归

M1 修改后原 M0 Benchmark 仍可运行：

```bash
go run ./cmd/benchmark -servers 100 -capacity 1000 -players 10000 -workers 8
```

本轮回归：

```text
Requested: 10,000
Successful: 10,000
Failed: 0
Retries: 0
```

具体吞吐/P99 会受当前容器调度影响，因此不作为跨版本性能结论。

## 5. M1 是否完成？

是。

M1 已建立：

```text
GameServer
   ↓ Register / Heartbeat
Registry
   ↓ Lease / TTL
Failure Detection
   ↓ Immutable View
Scheduler
```

下一阶段可以在稳定 Candidate Source 上正式进行 **RoundRobin vs LeastConnection vs LeastLoad** 对照实验。
