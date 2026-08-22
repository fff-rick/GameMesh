# 02. GameMesh 可行性调研

> 调研日期：2026-08-21

## 1. 结论摘要

**结论：项目可行。**

但推荐的实现方式不是“用 Go 重写现有基础设施”，而是：

- Go 自研：Gateway 热路径、游戏语义 Router、Balancer、Adaptive Controller、Policy Engine、Scheduler；
- 集成成熟组件：Kubernetes、Agones、Prometheus；
- 可选集成：Open Match、Envoy；
- 插件化：控制面优先采用进程隔离插件，Data Plane 热路径优先采用进程内接口。

GameMesh 的创新点应该放在**游戏语义、自适应策略与系统协同**，而不是重新实现容器编排、完整代理或完整 Matchmaking 框架。

---

## 2. Go 是否适合

### 2.1 适合的原因

GameMesh 的核心工作负载包括：

- 大量长连接；
- 高并发网络 I/O；
- 状态与指标采集；
- RPC；
- 调度控制循环；
- Kubernetes Controller / API Client；
- 插件与策略系统；
- 高并发压测工具。

这些都非常符合 Go 的工程优势：goroutine、成熟网络库、gRPC/Kubernetes 生态、较低部署复杂度和较好的服务端可维护性。

### 2.2 Go 的边界

Go 不应承担：

- 游戏渲染；
- GPU / Codec 热路径；
- 极端严格无 GC 停顿的实时战斗核心；
- 内核级网络功能（除非后续单独引入 eBPF / XDP 等）。

因此 GameMesh 的架构应让 Go 负责网络与调度基础设施，而不是游戏引擎内部的实时模拟。

---

## 3. Agones 调研

### 3.1 已有能力

Agones 是一个基于 Kubernetes 的 Dedicated Game Server 托管、运行与扩缩容方案。

截至本次调研，官方文档展示的版本为 **1.60.0**。其主要能力包括：

- GameServer 生命周期管理；
- Fleet；
- GameServerAllocation；
- 健康检查；
- Fleet Autoscaler；
- 多种 Scheduling / Autoscaling 模式；
- GameServer SDK；
- 指标；
- 外部 Allocator Service；
- 多集群相关能力。

### 3.2 对 GameMesh 的意义

这意味着 GameMesh **没有必要自己负责容器生命周期和底层 Fleet 管理**。

更合理的关系是：

`GameMesh Scheduler -> Agones Allocator / Kubernetes API -> GameServer Fleet`

GameMesh 决定“为什么扩、扩多少、哪类服务器更适合当前 Match”；Agones 决定“如何在 Kubernetes 中管理并分配 Dedicated GameServer”。

### 3.3 Fleet Autoscaler

Agones FleetAutoscaler 已支持 Ready Buffer、Webhook、Wasm、Schedule、Chain 等策略。

因此 GameMesh 的扩缩容策略应优先通过：

- Webhook；
- Agones API；
- 外部 Controller；

向 Agones 输出 Desired Capacity，而不是复制 FleetAutoscaler 的底层能力。

### 3.4 GameServerAllocation

Agones 支持从多个 GameServer 中原子分配合适实例，并将其状态切换为 Allocated。

这与 GameMesh 的 Scheduler 高度互补：

- GameMesh 负责业务策略、Region、Party、GameMode、压力状态；
- Agones 负责 GameServer 生命周期与最终 Allocation。

### 3.5 Player Capacity

Agones 已提供 Counters / Lists 以及基于容量的 Allocation 能力，但官方标记部分能力仍为 Beta，并明确提示其系统是 eventual consistency。

因此 GameMesh 不应把“毫秒级实时玩家容量”完全依赖 Kubernetes CRD 状态。

建议：

- Data Plane / Scheduler 保留本地近实时容量视图；
- Agones 作为生命周期与最终分配权威；
- 允许短暂不一致，并在 Allocation 失败时重试选择。

### 3.6 结论

**强烈建议集成 Agones，而不是重做 Agones。**

它会显著提升项目的工程真实性，同时让自研部分集中在真正有辨识度的调度与自适应算法上。

---

## 4. Kubernetes HPA 调研

Kubernetes HPA 可以根据 CPU、Memory 和自定义指标水平扩缩 Deployment / StatefulSet 等资源。

官方文档指出：HPA 本质上是一个周期性 Control Loop，默认同步周期为 **15 秒**。

### 对 GameMesh 的影响

对于普通 Web 服务，15 秒控制周期通常可以接受；但游戏开服、断线重连、热门活动等场景可能在数秒内产生巨大流量变化。

因此仅依赖：

`CPU > 70% -> HPA 扩容`

可能出现：

1. HPA 发现压力需要时间；
2. Pod 创建需要时间；
3. GameServer 初始化 / Warmup 需要时间；
4. 在新容量 Ready 前，旧实例已经过载。

### GameMesh 的补充价值

GameMesh 可以做两件 HPA 不负责的事：

1. **短时间保护**：限流、排队、Load Shedding、停止向高负载实例分配；
2. **更早决策**：根据连接增速、队列增速、Ready Buffer 等指标提前请求扩容。

所以 GameMesh 与 HPA 是互补关系，不是替代关系。

---

## 5. Envoy 调研

### 5.1 已有能力

Envoy 已经具有成熟的通用流量治理能力，例如：

- Weighted Round Robin；
- Least Request；
- Ring Hash；
- Maglev；
- Health Checking；
- Circuit Breaking；
- Overload Manager；
- 动态 xDS 配置。

其 Overload Manager 能够监测 CPU、内存、文件描述符等资源，并根据压力触发停止接收请求或 TCP 连接等动作。

### 5.2 为什么仍然有 GameMesh 的空间

Envoy 理解的是：

- downstream；
- upstream；
- request；
- connection；
- endpoint。

GameMesh 希望进一步理解：

- Player；
- Party；
- Room；
- Match；
- GameMode；
- GameServer Capacity；
- Tick Latency；
- Region；
- Ready GameServer Buffer。

因此 GameMesh 不应该宣传成“比 Envoy 更好的通用代理”，而应该宣传成：

**“建立在通用代理/编排能力之上的游戏语义自适应调度层”。**

### 5.3 两种实现路线

#### 路线 A：MVP 自研轻量 Data Plane

优势：

- 能充分展示 Go 网络编程；
- 容易做实验；
- 可完整掌控路由算法。

缺点：

- 需要自己承担连接管理、限流、健康检查等基础工程。

#### 路线 B：Envoy Data Plane + Go Control Plane

优势：

- 复用成熟代理能力；
- 更接近 Service Mesh 思路。

缺点：

- 自研网络代码减少；
- xDS 与 Envoy 配置复杂度增加；
- 项目容易变成“配置 Envoy”，削弱 Go 热路径亮点。

### 建议

**MVP 采用路线 A；高级版本再增加 Envoy Adapter / xDS 实验。**

---

## 6. Open Match 调研

Open Match 是用于构建可扩展 Matchmaker 的开源框架，其目标是解决大规模 Ticket、查询和并发 Match Generation 等通用问题，同时让开发者自定义 Matchmaking Logic。当前项目已有 MMO 负责这一业务边界，因此 Open Match 不再是 GameMesh 的集成目标。

### 6.1 与 GameMesh 的关系

在没有 MMO 的独立部署中，Open Match 可负责：

- Match Ticket；
- Pool；
- Match Function；
- Evaluator；
- Assignment 工作流。

GameMesh 更适合负责：

- 将 Match 映射到合适 GameServer；
- 服务器容量与 Region 调度；
- Gateway 路由；
- 扩缩容与过载保护。

与 MMO 集成时，目标关系是：

`MMO Matchmaking -> GameMesh Runtime Layer -> Agones Allocator -> GameServer`

### 6.2 当前风险

公开 release 页面中能看到的最新版本为 v1.8.1，其描述的测试环境主要是 Kubernetes 1.24 / 1.25，核心开发要求 Go >= 1.21。官方 Overview 页面最近修改时间也明显早于 Agones 当前文档。

这意味着：

- 其设计仍值得参考；
- 但不适合把 MVP 的核心架构锁死在 Open Match 上；
- 不应作为 GameMesh 的 Adapter 接入；
- 不应在 GameMesh 内维护第二套 Match Simulator / Simple Matchmaker。

### 6.3 结论

**Open Match = 独立部署时的参考方案；与 MMO 集成时不接入。**

---

## 7. 插件机制调研

HashiCorp `go-plugin` 使用独立进程 + RPC / gRPC 实现插件系统，被 Terraform、Vault、Nomad 等工具使用。

优点：

- 插件 panic 不会直接导致宿主进程崩溃；
- 可以使用 gRPC；
- 可以跨语言；
- 插件生命周期独立；
- 支持校验与 TLS 等机制。

缺点：

- 跨进程 RPC 有额外开销；
- 不适合放在“每条游戏消息都执行一次”的极端热路径。

### GameMesh 的设计结论

采用“双插件模型”：

**Data Plane 热路径**

- 进程内 Interface；
- 编译期注册；
- 极少内存分配；
- 无额外 RPC。

**Control Plane 策略插件**

- 可使用 gRPC / go-plugin；
- 允许进程隔离；
- 适合 Autoscaler、Predictor、Infrastructure Adapter。

这比“所有模块都做动态插件”更合理。

---

## 8. 自研与复用边界

| 能力 | 建议 | 原因 |
|---|---|---|
| Go Gateway | 自研 | 展示网络与高并发核心能力 |
| Semantic Router | 自研 | 项目核心差异点 |
| Load Score | 自研 | 游戏语义核心 |
| Adaptive State Machine | 自研 | 项目核心差异点 |
| Overload Policy | 自研 | 可做压测对照实验 |
| Predictive Scaling | 自研 | 高级亮点 |
| GameServer Lifecycle | Agones | 已有成熟方案 |
| Pod / Node 编排 | Kubernetes | 不应重复造轮子 |
| Metrics | Prometheus | 标准生态 |
| 通用代理能力 | MVP 自研最小集；后期可接 Envoy | 平衡学习价值与工程成本 |
| Player Matchmaking | MMO | 避免 Player/Party/Ticket/Match 的双事实源 |
| 动态策略插件 | go-plugin / gRPC 可选 | 适合控制面 |

---

## 9. 主要技术风险

### 风险 1：范围失控

如果同时实现完整 Gateway、Matchmaker、Kubernetes Controller、Agones 替代品、Envoy 替代品，项目必然失控。

**处理：严格限定 MVP。**

### 风险 2：插件化影响性能

如果路由算法每次都通过 RPC 插件执行，P99 可能明显恶化。

**处理：热路径进程内，控制面才允许进程插件。**

### 风险 3：GameServer 指标不实时

Kubernetes / Agones 状态存在 eventual consistency。

**处理：本地指标缓存 + 心跳 + 最终 Allocation 二次校验。**

### 风险 4：扩容来不及

任何 Autoscaling 都受启动时间影响。

**处理：Ready Buffer + Predictive Trigger + Admission Control。**

### 风险 5：Session / Battle 迁移复杂

运行中 Match 无损迁移远超 MVP 范围。

**处理：第一版只迁移新流量，不迁移运行中 Battle。**

### 风险 6：项目变成“框架拼装”

如果主要工作只是安装 Kubernetes、Agones、Prometheus，会缺乏技术辨识度。

**处理：必须保留自研 Data Plane、Semantic Router、Adaptive Controller 和实验对照。**

---

## 10. 可行性评分

| 维度 | 评分 | 说明 |
|---|---:|---|
| Go 技术匹配度 | 9/10 | 网络、并发、Controller、RPC 都适合 |
| 工程真实性 | 9/10 | 与真实游戏基础设施问题高度吻合 |
| 可压测性 | 10/10 | 可构造玩家、服务器、故障、流量洪峰 |
| 项目辨识度 | 9/10 | 少见于普通学生项目 |
| MVP 可控性 | 8/10 | 需严格控制范围 |
| 完整产品难度 | 10/10 | 若追求生产级完整度，工程量巨大 |

**总体建议：立项。**
