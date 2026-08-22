# MMO 与 GameMesh 集成边界

## 决策

将“玩家如何组成一局”和“已成局对局如何运行”拆成两个独立问题：

```text
MMO Matchmaking
  Player / Party / Ticket -> 已成局 Match
        │ match_id、region、version、连接票据
        ▼
GameMesh Runtime Layer
  准入、健康隔离、会话路由、容量/负载策略
        │ 合格的 Fleet / Region 约束
        ▼
Agones
  原子 GameServerAllocation -> 地址与实例生命周期
```

## 唯一职责

| 系统 | 事实源与职责 | 明确不负责 |
| --- | --- | --- |
| MMO | 玩家、Party、Ticket、5V5 分队、匹配质量、Match 生命周期、评分、连接票据 | Gateway 流量转发、实例健康读模型 |
| GameMesh | Gateway 连接生命周期、票据校验、Match Affinity 路由、Registry 健康/容量读模型、准入与过载保护 | 玩家组队、MMR/Elo、Ticket 队列、创建 Match |
| Agones | Fleet 生命周期与具体 GameServer 的最终原子分配 | 玩家匹配、业务 Match 状态 |

GameMesh 可以根据 Registry 的 Region、版本、负载和健康度决定是否接受新 Match，或
选择候选 Fleet/Region；但不得凭本地 Snapshot 非原子地“占用某一台”实例。最终实例
分配必须通过 Agones 完成。MMO 将分配结果持久化为 Match Binding；GameMesh 仅消费该
绑定进行连接路由，不得自行改写或迁移运行中的 Match。

## 最小接口

MMO 向 GameMesh 发送已成局的放置请求：

```text
PlaceMatch(match_id, region, version, required_slots, connect_ticket)
```

GameMesh 返回准入结果或通过其 Agones Adapter 返回实例地址。重复的 `match_id` 必须
由 MMO 的业务幂等边界处理；Agones 是最终实例分配原子性的权威。客户端随后携带 MMO
签发的短期 `connect_ticket` 连接 GameMesh Gateway，Gateway 验票后严格路由到已保存的
Match Binding。

## 故障语义

- `UNHEALTHY` 实例不接受新的 Match 或新的未绑定连接；
- 已运行 Match 不会因 LoadScore 或临时指标波动被迁移；
- 实例在 Match 开始前失败：MMO 取消/重试该次放置；
- 实例在 Match 运行中失败：MMO 决定业务失败、补偿或重连策略，GameMesh 只执行连接层
  的拒绝、退避与可观测性。

## 非目标

GameMesh 不引入第二套 Match Ticket、队伍形成、评分、Match Repository、Open Match
集成或业务排队系统。这些能力与 MMO 重复，且会产生双事实源。
