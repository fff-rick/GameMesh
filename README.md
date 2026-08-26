# Game Gateway

当前完成版本：**Stage 2 — 鉴权、Session 与心跳**。

项目严格按 `docs/03-阶段性任务.md` 顺序开发：Stage 0、Stage 1、Stage 2 已验收，尚未进入 Stage 3。

## 目录

```text
game-gateway-stage2/
├── api/proto/envelope.proto
├── cmd/
│   ├── gateway/
│   └── bench/
├── internal/
│   ├── auth/          # Authentication Hook + 本地开发鉴权器
│   ├── config/
│   ├── gateway/       # Connection + Server + auth/heartbeat orchestration
│   ├── metrics/
│   ├── protocol/      # Envelope + Stage 2 control payload wire codec
│   ├── session/       # 并发安全 Session Manager
│   └── ws/
└── docs/
    ├── stage-0/
    ├── stage-1/
    ├── stage-2/
    └── superpowers/plans/
```

## 构建与测试

项目仍无第三方 Go module 依赖，可离线构建：

```bash
go test ./...
go test -race ./...
go vet ./...
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

## Stage 2 本地鉴权约定

默认 `DevAuthenticator` 仅用于演示 Hook，Token 格式：

```text
user:<UserID>
```

例如 `user:alice`。生产环境必须通过 `gateway.WithAuthenticator(...)` 注入真正的 Token 校验器，不能把该开发格式当作安全认证方案。

控制 MessageType：

```text
10 AuthRequest
11 AuthResult
12 HeartbeatRequest
13 HeartbeatResponse
```

默认 Heartbeat 扫描间隔 15 秒，Idle Timeout 45 秒。

## 重复登录

Stage 2 冻结策略为 **New Login Wins**：同账号新登录成功后，旧 Session 终止、旧 Connection 关闭，最终只有一个 ACTIVE Session。

## 下一阶段

Stage 3 才加入 gRPC 内部接口、MessageType -> Backend 路由、User -> Room、Room -> BackendInstance、RPC timeout 和基础快速失败策略。
