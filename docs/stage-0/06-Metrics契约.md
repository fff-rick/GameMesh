# 阶段 0：Metrics 契约

## 1. 原则

-   Metrics 名称稳定、低基数。
-   禁止将 UserID、SessionID、ConnID、RequestID、RoomID 直接作为
    Prometheus label。
-   ID 用于日志/Trace 关联，不进入高基数指标。
-   延迟优先 Histogram，当前量优先 Gauge，累计事件优先 Counter。

## 2. 核心 Metrics

建议统一前缀：`game_gateway_`

  ------------------------------------------------------------------------------------------------------------
  Metric                                      类型              建议 Labels                  含义
  ------------------------------------------- ----------------- ---------------------------- -----------------
  game_gateway_connections                    Gauge             gateway_id,state             当前连接数

  game_gateway_connections_total              Counter           gateway_id,result            建连累计

  game_gateway_disconnects_total              Counter           gateway_id,reason            断连累计

  game_gateway_messages_received_total        Counter           gateway_id,class             收到消息数

  game_gateway_messages_sent_total            Counter           gateway_id,class             发出消息数

  game_gateway_message_bytes_received_total   Counter           gateway_id                   入站字节

  game_gateway_message_bytes_sent_total       Counter           gateway_id                   出站字节

  game_gateway_message_forward_seconds        Histogram         backend_type,message_class   Gateway 转发延迟

  game_gateway_send_queue_depth               Histogram         gateway_id                   Send Queue
                                                                                             深度采样

  game_gateway_messages_dropped_total         Counter           gateway_id,reason,class      丢弃消息

  game_gateway_heartbeat_timeouts_total       Counter           gateway_id                   心跳超时

  game_gateway_reconnect_attempts_total       Counter           gateway_id,result            恢复尝试

  game_gateway_backend_rpc_seconds            Histogram         backend_type,method          下游 RPC 延迟

  game_gateway_backend_rpc_total              Counter           backend_type,method,result   RPC 结果

  game_gateway_sessions                       Gauge             gateway_id,state             Session 数

  game_gateway_shutdown_in_progress           Gauge             gateway_id                   是否正在 Shutdown

  game_gateway_shutdown_seconds               Histogram         gateway_id,result            Shutdown 耗时
  ------------------------------------------------------------------------------------------------------------

## 3. 性能观测必记项

每轮压测除业务 Metrics 外必须记录：

-   并发连接数；
-   每秒消息数；
-   P50/P95/P99 转发延迟；
-   CPU；
-   RSS；
-   GC 次数/暂停；
-   goroutine 数；
-   单连接平均内存估算；
-   Send Queue 堆积；
-   错误率/断连率。

## 4. 阶段启用顺序

阶段 0 只冻结名称与语义。实际埋点随实现阶段启用：

-   阶段 1：Connection、消息吞吐、字节、Send Queue；
-   阶段 2：Session、Heartbeat；
-   阶段 3：Backend RPC；
-   阶段 4：可靠消息 Pending/Retry/Dedup（届时扩充）；
-   阶段 5：Reconnect；
-   阶段 7：Drop/RateLimit/Shutdown。
