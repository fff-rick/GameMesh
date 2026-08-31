# Game Gateway

当前完成版本：**Stage 7 — 背压、限流与优雅下线（PASS）**。

项目严格按 `docs/03-阶段性任务.md` 顺序开发：Stage 0–7 已完成阶段闭环。Stage 3 的真实 gRPC 集成由项目负责人在外部环境确认通过；Stage 4 已实现 MessageID / Seq / ACK / Pending / Retry / Dedup 和可靠性分类；Stage 5 在单个 Gateway 进程内实现了短暂断线后的 Session 恢复；Stage 6 使用 Redis Lease 协调多 Gateway 用户归属；Stage 7 增加背压、限流与有期限 Drain。

## 核心目录

```text
game-gateway-stage5/
├── api/proto/
│   ├── envelope.proto
│   └── backend.proto
├── cmd/
│   ├── gateway/
│   └── bench/
├── internal/
│   ├── auth/
│   ├── backend/
│   ├── config/
│   ├── gateway/
│   ├── metrics/
│   ├── presence/      # Stage 6 Redis User -> Gateway TTL/Lease
│   ├── protocol/
│   ├── reliability/   # Stage 4 Seq/ACK/Pending/Retry/Dedup
│   ├── routing/
│   ├── session/
│   └── ws/
└── docs/
    ├── stage-0/
    ├── stage-1/
    ├── stage-2/
    ├── stage-3/
    ├── stage-4/
    ├── stage-5/
    ├── stage-6/
    ├── stage-7/
    └── superpowers/plans/
```

## 构建与测试

```bash
go test ./... -count=1
go test -race ./... -count=1
go vet ./...
go build ./...
```

## 运行

```bash
go run ./cmd/gateway -listen :8080 -gateway-id gateway-1
```

接口：

```text
WebSocket: ws://127.0.0.1:8080/ws
Health:    http://127.0.0.1:8080/healthz
Metrics:   http://127.0.0.1:8080/metrics
```

## Stage 2 鉴权

默认 `DevAuthenticator` 仅用于开发演示，Token 格式为 `user:<UserID>`。生产环境必须通过 `gateway.WithAuthenticator(...)` 注入真实校验器。重复登录策略仍为 **New Login Wins**。

## Stage 3 Backend 路由

Gateway 按：

```text
MessageType -> BackendType
UserID      -> RoomID
RoomID      -> BackendInstance
```

完成下游路由，业务 Payload 不在 Gateway 内解释。gRPC service 契约位于 `api/proto/backend.proto`。

## Stage 4 可靠消息

新增控制 MessageType：

```text
15 ACK
```

可靠消息必须显式通过 `reliability.Classifier` 分类。核心默认值：

```text
ReliableRetryInterval = 500ms
ReliableMaxRetries    = 3
ReliablePendingLimit  = 128
ReliableDedupWindow   = 256
```

语义详见 `docs/stage-4/01-可靠消息语义.md`。Stage 4 只保证当前 Session/Connection 生命周期内的可靠状态；断线恢复 Pending 属于 Stage 5。

## Stage 5 断线恢复

认证成功会下发 Resume Token。连接断开后，Session 进入默认 1 分钟的 Grace Period；客户端在窗口内通过携带 `last_ack_seq` 的 `ResumeRequest` 恢复原 Session。恢复成功后服务端会轮换 Token，先应用累计 ACK，再按原始 Seq 顺序重放未 ACK 的可靠消息。Session 会保留最后一次成功解析的 Room 路由，以恢复 Gateway 内的 `User -> Room` 映射；不会恢复 Backend 游戏状态。

恢复仅适用于同一、仍在运行的 Gateway 进程：不支持 Gateway 重启、跨 Gateway 或跨节点恢复。完整协议与故障边界见 `docs/stage-5/01-断线恢复语义.md`。

## Stage 6 多节点 Gateway

启用 `-presence-redis` 后，Gateway 会将仅有的分布式状态——`UserID -> GatewayID / ConnID`——保存为 Redis 短租约。Claim、Renew 和 Release 均通过 LeaseToken fencing，旧节点的延迟操作不能覆盖新归属；TTL 会在节点崩溃后自动清理。跨节点重复登录执行 **New Login Wins**，并通过 best-effort Pub/Sub 快速关闭旧连接。

Redis 短暂故障不会关闭已有连接；仅新的认证或 Resume 会得到可重试的 `presence_unavailable`。Session、Resume Token 和可靠消息状态仍不跨节点迁移，因此 Resume 仍必须到达原 Gateway。详见 `docs/stage-6/01-多节点语义.md`。

```bash
go run ./cmd/gateway -listen :8080 -gateway-id gateway-a -presence-redis 127.0.0.1:6379
```

## Stage 7 背压、限流与优雅下线

Gateway 对入站采用连接级和全局 token bucket，对下游 RPC 使用无等待的最大在途并发阀门；非可靠业务下行在队列满时可丢弃，可靠与控制消息则关闭慢连接。SIGTERM 会先停止接入并进入 Drain，给已接收请求默认 10 秒完成时间，期满后明确关闭剩余连接。

完整策略与参数见 `docs/stage-7/01-背压限流与下线语义.md`。

## 下一阶段

Stage 8：独立项目最终验收。
