# Gateway ↔ State Sync v1：接口契约草案

## 决策

State Sync 的 Snapshot 是与客户端请求独立的 Tick 驱动输出，**不接入**现有 unary `BackendService.Handle`。推荐在版本化包 `gsss.state_sync.v1` 中定义一条长期双向 gRPC Stream：

```proto
service StateSyncGateway {
  rpc Connect(stream GatewayToStateSync) returns (stream StateSyncToGateway);
}
```

`GatewayToStateSync` 与 `StateSyncToGateway` 复用 GSSS 已有 Proto 的 oneof。双向 Stream 保持单条 Stream 内顺序，适合持续 Input 与异步 Snapshot；连接重建后由应用层 Join/Leave 与 Snapshot baseline 语义恢复。

## 映射与所有权

| Gateway | State Sync | 规则 |
| --- | --- | --- |
| 已认证 `UserID` | `PlayerID` | 由可信 Match/身份服务稳定映射；禁止客户端自报 PlayerID |
| `RoomID` | `MatchID` | Room 路由解析后写入；同一 Session 同时只属于一个 Match |
| Session 建立/恢复 | `PlayerJoin` | 幂等；Resume 不创建第二个 Player |
| 连接最终离开/Grace 过期/顶号 | `PlayerLeave` | 携带明确原因；短暂断线在 Grace 内不反复 Join/Leave |
| 客户端输入 | `PlayerInput` | Gateway 只校验协议和身份，不解释移动/射击 |
| 客户端 Snapshot ACK | `SnapshotAck` | 仅转发给所属 Match 与 Player |
| Tick 输出 | `Snapshot` / `ControlEvent` | Gateway 仅定向投递给当前 Session |

## 传输策略

- `PlayerInput`：允许不可靠、顺序由 `input_seq` 在 State Sync 判定；Gateway 限流后转发。
- `Snapshot`：默认 best-effort/latest-wins；满队列可丢弃旧状态，客户端通过后续 Full Snapshot 恢复。
- Join、Leave 与关键 Control：可靠或明确失败，不能静默丢弃。
- Snapshot ACK 不是 Gateway Envelope 的可靠 ACK；两者职责不同，前者确认 Delta baseline，后者确认传输可靠消息。

## 失败与背压

Stream 断开时 Gateway 停止向该 State Sync 实例堆积请求，并将新输入映射为可重试 `backend_unavailable`。重连后重建 Stream，并按当前 Session/Room 重新发幂等 Join。State Sync 输出必须经 Gateway 的既有有界发送队列；慢客户端只影响自己的 Snapshot，不能阻塞 Tick 或其他 Player。

## 集成验收

最小端到端场景：两个已认证玩家加入同一 Match；输入只改变权威世界；各自只收到自己的 AOI Snapshot；ACK 后产生 Delta；一方断线、Grace 内 Resume 不产生重复 Join；Stream/Backend 故障返回可重试错误；Gateway Drain 会停止新 Input 并按策略 Leave。

本草案冻结职责与语义，不冻结 GSSS 的具体 Go module 路径或部署发现方式；两项目合并前应在共享 Proto 仓库生成代码并进行 breaking-change 检查。
