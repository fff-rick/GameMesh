# Gateway v1：Metrics 清单

所有指标均由 `/metrics` 以 Prometheus 文本格式输出。`gateway_id` 是稳定实例标签；绝不将 UserID、ConnID、SessionID 或 Resume/Lease Token 放入 label。

| 指标族 | 用途 |
| --- | --- |
| `game_gateway_connections*` | 当前连接、累计接入与按原因关闭 |
| `game_gateway_messages_*` / `game_gateway_message_bytes_*` | Envelope 与字节吞吐 |
| `game_gateway_send_queue_depth_max` | 发送队列历史最高深度 |
| `game_gateway_sessions*` / `game_gateway_session_grace_expired_total` | 活跃/Grace Session 与过期回收 |
| `game_gateway_auth_total` / `game_gateway_recovery_total` | 鉴权与恢复结果 |
| `game_gateway_heartbeat_timeouts_total` | 僵尸连接回收 |
| `game_gateway_reliable_*` | Pending、Retry、Dedup、乱序、耗尽与溢出 |
| `game_gateway_backend_rpc_*` | Backend 调用结果与耗时 |
| `game_gateway_presence_*` | Redis Lease 数和 Claim/Renew/Release/Eviction 结果 |
| `game_gateway_rate_limited_total` | 连接级或全局令牌桶拒绝 |
| `game_gateway_messages_dropped_total` | 非可靠消息在满队列时的丢弃 |
| `game_gateway_backend_rejected_total` | Backend 在途并发阀门拒绝 |
| `game_gateway_shutdown_total` | Drain 开始、完成、超时与 Drain 中业务拒绝 |

告警建议：连接数持续上升、队列深度接近 `SendQueueSize`、可靠 Pending 溢出、Backend timeout/unavailable、Presence renew error、Drain timeout，以及任意 rate-limit/drop/reject 的持续非零增长。
