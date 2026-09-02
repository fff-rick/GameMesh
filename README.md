# GameMesh

GameMesh 是一个使用 Go 实现的游戏接入层（Game Gateway）。它为游戏客户端提供 WebSocket 长连接接入，并负责认证、会话管理、心跳、消息可靠性、后端路由、跨节点在线归属和优雅下线；游戏规则与权威世界状态仍由后端服务负责。

项目当前已完成 Gateway v1 的阶段性验收（Stage 0–8），可作为独立的 Gateway 示例运行。

## 架构与能力

```text
Game Client
    │ WebSocket + Protobuf Envelope
    ▼
GameMesh Gateway
    ├── 认证、Session、心跳、断线恢复
    ├── ACK / 重试 / 去重、背压与限流
    ├── User → Room → Backend 路由
    └── 可选 Redis 在线归属（多 Gateway）
    │ gRPC
    └──────────────► 业务 Backend / Match / Chat
```

- **统一协议**：客户端通过二进制 WebSocket 传输 Protobuf `Envelope`；协议定义位于 `api/proto/envelope.proto`。
- **接入与会话**：内置鉴权接口、心跳与空闲连接回收；重复登录采用 **New Login Wins**。
- **可靠消息**：支持 `MessageID`、`Seq`、`ACK`、待确认队列、重试和去重窗口。
- **断线恢复**：认证后签发 Resume Token；默认可在 1 分钟宽限期内恢复同一 Gateway 进程中的 Session。
- **后端路由**：Gateway 仅转发，不解释业务 Payload；通过 `MessageType → BackendType`、`UserID → RoomID`、`RoomID → BackendInstance` 选择后端。
- **多节点归属**：可选 Redis Lease 管理 `UserID → GatewayID / ConnID`，支持跨节点重复登录的归属切换。
- **生产保护**：连接级/全局限流、下游最大并发、慢客户端背压策略，以及 SIGTERM 的有期限 Drain。
- **可观测性**：提供健康检查与 Prometheus Metrics 端点。

> 重要边界：Resume Token、Session 和可靠消息状态目前不会跨进程迁移。因此断线恢复必须回到原 Gateway，且不支持 Gateway 重启后恢复；它也不会恢复后端的游戏状态。

## 环境要求

- Go **1.25+**
- 可选：Redis（仅多 Gateway 在线归属场景需要）

## 快速开始

进入项目并下载依赖：

```bash
cd /home/xin/work/GameMesh
go mod download
```

启动 Gateway：

```bash
go run ./cmd/gateway -listen :8080 -gateway-id gateway-1
```

服务启动后可访问：

| 地址 | 用途 |
| --- | --- |
| `ws://127.0.0.1:8080/ws` | 客户端 WebSocket 接入点（二进制帧） |
| `http://127.0.0.1:8080/healthz` | 健康检查 |
| `http://127.0.0.1:8080/metrics` | Prometheus Metrics |

按 `Ctrl+C` 或发送 `SIGTERM` 可触发优雅下线：Gateway 先停止新接入，再等待已接收的请求在默认 10 秒内完成。

## 客户端接入

WebSocket 仅接受 **二进制**帧，帧内容是 Protobuf 编码的 `gamegateway.v1.Envelope`。最小交互流程如下：

```text
Client                       GameMesh
  │ ── AuthRequest ───────────► │
  │ ◄─ AuthResult + ResumeToken ─│
  │ ── Heartbeat / 业务消息 ───► │
  │ ◄─ 响应 / ACK / 业务下行 ───│
```

开发环境默认鉴权器使用以下 Token 格式：

```text
user:<UserID>
```

例如 `user:alice`。生产环境请在构造 Gateway 时通过 `gateway.WithAuthenticator(...)` 注入真实的鉴权实现，勿使用默认开发鉴权器。

主要控制消息类型：

| MessageType | 含义 |
| ---: | --- |
| 10 / 11 | `AuthRequest` / `AuthResult` |
| 12 / 13 | `HeartbeatRequest` / `HeartbeatResponse` |
| 14 | `Error` |
| 15 | `ACK` |
| 16 / 17 | `ResumeRequest` / `ResumeResult` |

完整字段和编号以 [envelope.proto](api/proto/envelope.proto) 为准。客户端对可靠下行消息应回传 ACK；恢复 Session 时需在 `ResumeRequest` 中携带最新的 `last_ack_seq`。

## 多 Gateway 模式（可选）

启动本地 Redis 后，为每个 Gateway 指定不同实例 ID 并连接同一 Redis：

```bash
go run ./cmd/gateway \
  -listen :8080 \
  -gateway-id gateway-a \
  -presence-redis 127.0.0.1:6379
```

Redis 仅保存短租约形式的在线归属。Redis 暂时不可用时，现有连接保持不受影响；新的认证或 Resume 请求会收到可重试的 `presence_unavailable` 错误。

## 常用运行参数

```bash
go run ./cmd/gateway -help
```

常用参数包括：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-listen` | `:8080` | HTTP / WebSocket 监听地址 |
| `-gateway-id` | `gateway-1` | Gateway 实例标识 |
| `-presence-redis` | 空 | Redis 地址；为空时关闭分布式在线归属 |
| `-connection-rate` | `1000` | 单连接每秒最大入站 Envelope 数 |
| `-global-rate` | `20000` | Gateway 全局每秒最大入站 Envelope 数 |
| `-backend-max-in-flight` | `1024` | 最大并发下游 RPC 数 |
| `-drain-timeout` | `10s` | 优雅下线最长等待时间 |

## 构建、测试与压测

```bash
# 单元与集成测试
go test ./... -count=1

# 竞态检测
go test -race ./... -count=1

# 静态检查与构建
go vet ./...
go build ./...

# 本地 Echo 压测（内嵌测试 Gateway）
go run ./cmd/bench -clients 50 -messages 100 -payload 256
```

如需验证 gRPC 路由，可启动 `cmd/fake_backend`：

```bash
go run ./cmd/fake_backend -listen 127.0.0.1:9091 -mode success
```

`-mode` 可选 `success`、`delay` 或 `business-error`，用于模拟后端正常响应、超时和业务错误。

## 目录结构

```text
GameMesh/
├── api/proto/          # Envelope 与内部 gRPC 协议
├── cmd/gateway/        # Gateway 可执行程序
├── cmd/fake_backend/   # 路由联调用 gRPC 后端
├── cmd/bench/          # 轻量 Echo 压测工具
├── internal/
│   ├── auth/           # 鉴权
│   ├── gateway/        # HTTP/WS 接入与生命周期
│   ├── session/        # Session、断线恢复
│   ├── reliability/    # Seq / ACK / Retry / Dedup
│   ├── routing/        # 用户、房间、后端路由
│   ├── presence/       # Redis Lease 在线归属
│   └── metrics/        # Prometheus 指标
└── docs/               # 设计、阶段任务、验收和测试报告
```

## 更多文档

- [方案设计](docs/02-方案设计.md)
- [阶段性任务与验收标准](docs/03-阶段性任务.md)
- [可靠消息语义](docs/stage-4/01-可靠消息语义.md)
- [断线恢复语义](docs/stage-5/01-断线恢复语义.md)
- [多节点 Gateway 语义](docs/stage-6/01-多节点语义.md)
- [背压、限流与下线语义](docs/stage-7/01-背压限流与下线语义.md)
- [最终验收文档](docs/stage-8/)
