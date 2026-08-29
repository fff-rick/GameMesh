# Stage 5 断线恢复设计

## 目标

在单个 Gateway 实例内，让已认证客户端在短暂断网后恢复原有 Session、可靠消息状态和既有路由上下文，而不重新认证或创建新的游戏上下文。

## 范围与约束

- 仅覆盖单节点内存状态；Redis 与跨 Gateway 恢复留给 Stage 6。
- 恢复请求仅使用服务端签发的 Resume Token，不要求再次提交认证 Token。
- Resume Token 是至少 128 位熵的、不透明的 CSPRNG 令牌；不写入日志，也不作为 Metrics label。
- Token 只能成功使用一次：恢复成功即轮换，旧 Token 立即失效。
- 每个 Session 在任意时刻只有一个有效连接。
- 断线 Grace Period 内，Session、可靠性 Seq/ACK/Pending 状态和 UserID 到 Room 的路由依据均保留；到期后统一释放。

## 会话模型

`Session` 增加 `ResumeToken`、`GraceDeadline` 和状态：

```text
ACTIVE -> DISCONNECTED_GRACE -> EXPIRED
  ^              |
  |              +-- Resume Token 有效且未过期
  +--------------+
```

连接意外关闭时，Session Manager 仅在关闭连接仍是该 Session 当前 ConnID 时转入 `DISCONNECTED_GRACE`；因此旧连接在新连接已恢复后关闭，不会删除或降级新绑定。

恢复操作在 Session Manager 的一个临界区内完成：严格查找已有 Resume Token、校验 GraceDeadline、绑定新 ConnID、清除 GraceDeadline、轮换 Token，并返回同一 Session。不存在“接受客户端给定 Token 并创建新 Session”的宽松模式。

普通认证维持既有 New Login Wins：新登录会终止旧 Session、使其 Resume Token 失效，并清除关联可靠性状态。

## 协议

新增两个控制消息：

```text
ResumeRequest { resume_token }
ResumeResult  { ok, session_id, resume_token, error_code }
```

- 初次认证成功的 `AuthResult` 增加 `resume_token`。
- 未认证连接只能发送 `AuthRequest` 或 `ResumeRequest`；恢复成功后连接立即成为已认证状态，并绑定原 `UserID` 与 `SessionID`。
- 无效、过期或已使用的令牌统一返回 `resume_token_invalid`，不泄漏具体失效原因。
- 已认证连接发送 ResumeRequest 返回 `already_authenticated`。
- 令牌字段使用当前 protobuf-wire 控制负载编码，并保持未知字段可跳过的兼容规则。

## 可靠消息和路由

- 可靠性 Manager 不在断线时删除 Session 状态；ACK、Seq、Dedup Window 与 Pending 原样保留。
- 恢复成功后，Gateway 按 Seq 升序重新投递全部 Pending Envelope，且复用 MessageID、Seq、MessageType、RequestID 和 Payload。
- `DISCONNECTED_GRACE` 期间 retry scanner 不生成重传也不消耗重试次数；恢复后按正常间隔继续处理。
- 路由不缓存于 Session：恢复后的业务消息仍使用既有 `UserID -> Room -> BackendInstance` Resolver，因而保留 Room 路由语义并可反映路由表更新。
- Backend 在恢复时不参与调用；恢复成功不受 Backend 可用性影响。后续业务请求按原有 backend timeout/unavailable 错误映射执行。

## 生命周期和回收

配置新增 `SessionGracePeriod`。Server 使用一个全局 session scanner，按固定检查间隔回收已到期的 Grace Session，并同时移除 Reliability Manager 中的对应状态。Server 主动 Shutdown 仍直接销毁全部状态，不进入恢复期。

新增可聚合 Metrics：恢复结果、Grace 中 Session 数、Grace 过期数。禁止将 UserID、SessionID、ConnID、Resume Token 或 RoomID 作为 label。

## 错误处理与并发

- 新恢复和旧连接关闭并发时，以 Session Manager 内原子 ConnID 替换为准；旧 close callback 无副作用。
- 两个连接同时使用同一 Token 时，最多一个成功；成功者令牌轮换后另一个失败。
- 恢复期间发送队列满按既有 `send_queue_full` 关闭策略处理；此时 Session 重新进入 Grace。
- Token 生成失败或 Session Manager 内部错误返回恢复失败，不绑定身份。

## 验证

测试以 TDD 覆盖：立即恢复、Grace 内恢复、Grace 后拒绝、Token 重放、并发恢复、旧新连接交叠、可靠 Pending 原序重放、路由恢复、Backend 不可用时恢复、Grace 到期释放和 race 条件。完成后执行：

```bash
go test ./...
go test -race ./...
```
