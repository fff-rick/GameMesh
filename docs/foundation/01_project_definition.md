# 01. GameMesh 项目定义

## 1. 项目背景

多人在线游戏在流量快速增长时，经常同时面临以下问题：

- 网关连接数、CPU、内存快速升高；
- 新玩家持续进入高负载 GameServer；
- 已成局 Match 的接入请求与重连迅速积压；
- 某些 Region、Room、Match 成为热点；
- 服务扩容存在启动与预热时间，CPU 高了再扩容往往已经偏晚；
- Retry / Reconnect 进一步放大故障，形成雪崩；
- 通用负载均衡器缺少 Party、Match、Room、Region 等游戏业务语义。

GameMesh 希望在游戏客户端与后端 GameServer 之间增加一个长期存在的自适应调度层，使系统在平峰、高峰和过载状态下采取不同策略。

---

## 2. 项目定位

**GameMesh = Game Runtime Gateway + Session Router + Adaptive Traffic Controller + GameServer Placement Policy**

它不是单纯的反向代理，也不是完整的游戏后端平台。与 MMO 集成时，MMO 是玩家、
Party、Ticket、5V5 分队、评分和 Match 生命周期的唯一事实源；GameMesh 只处理已成局
Match 的运行时接入、健康隔离、容量策略和流量保护。Agones 仍是最终原子实例分配与
生命周期的权威。

### 2.1 正常状态

负责：

- TCP / WebSocket（后续可扩展 QUIC / UDP）入口；
- 服务发现；
- 健康检查；
- Session Affinity；
- 基础负载均衡；
- GameServer 容量感知；
- 消费 MMO 已创建 Match Binding 的 Region / Match 路由；
- 指标采集。

### 2.2 高负载状态

负责：

- 停止向高压力 GameServer 分配新玩家；
- 根据健康、容量与过载策略接受或拒绝 MMO 的新 Match 放置请求；
- 动态调整路由权重；
- 触发 GameServer Fleet 扩容；
- Gateway 横向扩容，并向 MMO 输出容量压力信号；
- 热点 Region 的连接与已成局 Match 接入隔离；
- 预留 Ready GameServer Buffer。

### 2.3 过载状态

负责：

- Admission Control；
- 排队与等待室；
- 非核心 API 限流；
- Load Shedding；
- 慢连接 / 慢消费者隔离；
- Retry / Reconnect 抑制；
- 核心战斗流量优先级保护。

---

## 3. 主要使用场景

### 场景 A：新版本上线 / 活动开服

在线人数从 10 万在数分钟内增长到 50 万甚至更高。

GameMesh 根据连接增长率、MMO 输出的队列压力、Ready GameServer 数量、Tick Latency 等指标提前进入保护或扩容状态，而不是只等待 CPU 超阈值。

### 场景 B：部分服务器负载失衡

某些服务器 90% 负载，另一些只有 30%。

GameMesh 不再仅按 Round Robin 处理新连接。在共享 Lobby、Room 或 World Shard 等可复用实例场景，它可计算 Load Score 并放置新会话；5V5 专属实例的最终占用仍由 Agones 原子完成。

### 场景 C：已成局 Match 的语义路由

MMO 已决定 Party 如何组成 Match，并持久化 `match_id -> GameServer` Binding。

GameMesh 验证 MMO 签发的连接票据，并根据该 Binding 做 Session Affinity；它不形成 Party，也不重新选择运行中的 Match 的目标实例。

### 场景 D：服务器压力超过安全阈值

系统进入 Overload 状态后，不追求“100% 请求都成功”，而是通过限流、排队和降级保证核心 GameServer 不被打垮。

---

## 4. 核心模块

### 4.1 Gateway

负责客户端入口与连接生命周期。

计划能力：

- TCP；
- WebSocket；
- Heartbeat；
- Session；
- Connection Limit；
- Backpressure；
- Graceful Drain；
- Reconnect Jitter / Backoff 协议支持。

第一阶段不强求支持所有协议，优先 TCP + WebSocket。

### 4.2 Semantic Router

根据游戏上下文路由：

- `match_id`
- `region`
- `game_mode`
- `server_version`

这些字段是 MMO 已成局 Match 的放置约束或连接票据声明，不用于 GameMesh 自行组队或创建 Match。

用于实现“业务语义负载均衡”。

### 4.3 Load Balancer

内置策略：

- Round Robin；
- Weighted Round Robin；
- Least Connection；
- P2C / Least Load；
- Consistent Hash；
- Region Aware；
- Match Binding Aware（消费 MMO Binding，不创建 Binding）；
- Composite Load Score。

### 4.4 GameServer Registry

维护 GameServer 状态：

- Ready / Allocated / Draining / Unhealthy；
- CPU / Memory；
- Connections；
- Player Count；
- Room / Match Count；
- Tick Latency；
- Network；
- Version；
- Region；
- Capacity。

### 4.5 Adaptive Controller

以状态机方式描述系统压力：

- `LOW`
- `NORMAL`
- `HIGH`
- `OVERLOAD`
- `EMERGENCY`

根据状态执行不同动作。

### 4.6 Scheduler

负责：

- 为 MMO 已成局 Match 评估可用 Region/Fleet 与运行时准入；
- 为共享实例场景的新 Room/Session 选择候选池；
- 避免调度到即将 Drain 的实例；
- 根据版本 / Region / Capacity 过滤候选节点；
- 将合格约束交给 Agones 执行最终原子实例分配；

### 4.7 Autoscaling Policy Engine

支持：

- CPU Scaling；
- Connection Scaling；
- Queue Length Scaling；
- Ready Buffer Scaling；
- Composite Scaling；
- Predictive Scaling（高级阶段）。

### 4.8 Overload Protector

支持：

- Rate Limit；
- Admission Control；
- Queue；
- Circuit Breaker；
- Load Shedding；
- Priority；
- Graceful Degradation。

### 4.9 Observability

至少提供：

- Prometheus Metrics；
- 结构化日志；
- Trace ID；
- Routing Decision 指标；
- Scaling Decision 指标；
- GameServer Load Distribution 指标；
- P50 / P95 / P99 延迟。

---

## 5. 插件化目标

GameMesh 不应把所有实现写死在核心进程中，而是区分：

### 热路径插件

位于 Data Plane，要求极低开销。

适合：

- Balancer；
- Router；
- Admission Policy；
- Hash Strategy。

第一阶段优先采用**编译期注册 / 内置接口扩展**，避免跨进程 RPC 成为热路径开销。

### 控制面插件

不直接处理每个用户请求，可以容忍 RPC。

适合：

- Autoscaling Policy；
- Prediction Strategy；
- Infrastructure Adapter；
- MMO Placement / Match Binding Adapter。

后续可采用 gRPC / HashiCorp go-plugin 等进程隔离方式。

### 基础设施适配器

目标包括：

- Kubernetes Adapter；
- Agones Adapter；
- Prometheus Adapter；
- 静态 GameServer Registry Adapter。

---

## 6. 非目标（第一阶段明确不做）

以下内容非常复杂，不应进入 MVP：

- 游戏客户端 / 游戏引擎；
- 战斗逻辑；
- 物理同步；
- 帧同步 / 状态同步协议；
- 反作弊；
- 全球 Anycast 网络；
- CDN；
- 运行中 Battle 的无损跨 GameServer 状态迁移；
- 自研 Kubernetes 替代品；
- 自研完整 Envoy 替代品；
- 自研完整 Open Match 替代品。
- 玩家组队、Match Ticket、MMR/Elo、Match Repository；这些由 MMO 提供。

尤其是“运行中的一场 MOBA 对局动态迁移”需要 State Snapshot、复制、一致性与 Session Migration，是后续独立研究主题。

---

## 7. 项目最终希望证明什么

GameMesh 最终不是靠功能数量证明价值，而是靠对比实验：

1. 与 Round Robin 相比，是否能降低 GameServer 负载方差；
2. 突发流量下是否能减少过载实例数量；
3. 是否能在扩容完成前通过 Admission / Load Shedding 避免雪崩；
4. MMO 已创建的 Match Binding 是否能稳定地被 Gateway 路由到正确节点；
5. 在节点故障、扩缩容过程中，路由是否能快速收敛；
6. GameMesh 自身加入后，对请求 P99 延迟增加是否可控；
7. 预测式策略是否能比纯 CPU 阈值更早触发扩容。

这些数据将作为项目最终简历与技术报告的核心证据。
