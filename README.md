# Game Gateway

当前完成版本：**Stage 1 — 单节点长连接 Gateway**。

项目严格按 `docs/03-阶段性任务.md` 顺序开发：Stage 0 和 Stage 1 已验收，尚未进入 Stage 2。

## 目录

```text
game-gateway-stage1/
├── api/proto/envelope.proto
├── cmd/
│   ├── gateway/       # Gateway 可执行程序
│   └── bench/         # Stage 1 loopback 基线工具
├── internal/
│   ├── config/
│   ├── gateway/       # Connection + Server
│   ├── metrics/
│   ├── protocol/      # Protobuf wire Envelope
│   └── ws/            # 最小 RFC 6455 WebSocket 实现
└── docs/
    ├── 01-方案调研.md
    ├── 02-方案设计.md
    ├── 03-阶段性任务.md
    ├── stage-0/
    └── stage-1/
```

## 构建与测试

项目当前无第三方 Go module 依赖，可离线构建：

```bash
go test ./...
go test -race ./...
go vet ./...
```

## 运行 Gateway

```bash
go run ./cmd/gateway -listen :8080 -gateway-id gateway-1
```

接口：

```text
WebSocket: ws://127.0.0.1:8080/ws
Health:    http://127.0.0.1:8080/healthz
Metrics:   http://127.0.0.1:8080/metrics
```

## 性能基线

```bash
go run ./cmd/bench -clients 50 -messages 200 -payload 256
```

本次开发容器 loopback 基线约 105,963 msg/s，详细条件见 `docs/stage-1/03-性能基线.md`。

## 下一阶段

Stage 2 才会加入 Authentication Hook、ConnID/UserID/SessionID、Session Manager、重复登录策略、Heartbeat、Idle Timeout 和在线状态 Metrics。
