# Game Gateway

当前完成版本：**Stage 4 — 可靠消息（PASS）**。

项目严格按 `docs/03-阶段性任务.md` 顺序开发：Stage 0–4 已完成阶段闭环。Stage 3 的真实 gRPC 集成由项目负责人在外部环境确认通过；Stage 4 已实现 MessageID / Seq / ACK / Pending / Retry / Dedup 和可靠性分类。

## 核心目录

```text
game-gateway-stage4/
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

## 下一阶段

Stage 5：断线恢复，包括 Resume Token、Session Grace Period、LastAckSeq、Pending Message 恢复、Room 路由恢复和 Session 过期回收。
