# 04. MVP 与验证计划

## 1. MVP 的目标

MVP 不是做出一个完整商业游戏平台，而是证明下面这句话：

> 在突发并发、负载不均和节点变化情况下，GameMesh 能比静态 Round Robin 更合理地分配新游戏流量，并在容量不足时保护系统不发生级联雪崩。

---

## 2. MVP 必须实现的能力

### M0：架构与模拟器

- GameServer Simulator 设计；
- Player / Party / Match 数据模型；
- GameServer 状态模型；
- 压测场景定义；
- 指标定义。

> 当前文档包属于 M0 的立项阶段，还未开始编码。

### M1：基础 Gateway 与 MMO Match 接入

- TCP 或 WebSocket 至少一种；
- Connection Lifecycle；
- Session；
- GameServer Registry；
- Round Robin Baseline。
- 消费 MMO 已成局 Match Binding；不实现玩家组队或 Ticket 队列。

### M2：Game-aware Balancer

- Least Connection；
- P2C；
- Load Score；
- Consistent Hash；
- Match Binding 路由（消费 MMO Binding）；
- Region / Version Filter。

### M3：Adaptive Controller

- LOW / NORMAL / HIGH / OVERLOAD 状态机；
- Threshold；
- Hysteresis；
- Cooldown；
- EWMA 指标平滑；
- Policy 变更记录。

### M4：过载保护

- Rate Limit；
- Admission Control；
- 连接准入队列（不管理 MMO 匹配队列）；
- Load Shedding；
- High Load GameServer Blackout；
- Retry / Reconnect 抑制策略。

### M5：Kubernetes / Agones 集成

- Kubernetes 部署；
- Agones Fleet；
- GameServer Allocation；
- GameServer Health；
- Ready Buffer；
- Scale Out / Scale In。

### M6：可观测性

- Prometheus；
- Grafana Dashboard；
- Routing Decision Metrics；
- Scaling Metrics；
- Load Distribution；
- P99；
- Reject / Queue / Shed Metrics。

### M7：预测扩容（高级）

- Connection Growth Rate；
- Match Queue Growth Rate；
- Simple Linear Prediction；
- Historical Window；
- 与纯 CPU Threshold 对比。

### M8：可插拔化与第三方集成（高级）

- Control Plane Plugin；
- Agones Adapter 完整抽象；
- 可选 Envoy / xDS PoC。

---

## 3. 第一版不要做的事

为了防止工程失控，MVP 禁止把下面内容加入 Must Have：

- QUIC + TCP + UDP + WebSocket 全协议同时做；
- 完整 Matchmaking 算法平台；
- Player / Party / Ticket / MMR / Elo / Match Repository（由 MMO 负责）；
- AI 预测模型；
- 多云多 Region；
- Battle Live Migration；
- 自研 Service Mesh；
- 自研 Kubernetes Scheduler；
- 自研完整时序数据库。

---

## 4. 基准实验设计

### 实验 A：负载均衡质量

**Baseline：** Round Robin

**GameMesh：** P2C / LeastLoad / LoadScore

观察：

- 各 GameServer Player Count 标准差；
- CPU / Connection Load 方差；
- 最大节点与平均节点负载比；
- Routing P99。

目标不是预设结论，而是通过数据证明哪种策略在哪些工作负载下更优。

---

### 实验 B：突发流量

场景：

- 正常 10k 在线；
- 在 30 秒内增加到 50k / 100k 模拟连接；
- GameServer Capacity 固定或动态扩展。

对比：

1. 无 GameMesh 保护；
2. GameMesh 只有 Load Balance；
3. GameMesh + Admission / Queue；
4. GameMesh + Autoscale。

观察：

- P99；
- Reject Rate；
- Timeout；
- Overloaded GameServer 数量；
- 系统恢复时间。

---

### 实验 C：节点故障

场景：

- 随机 Kill 10% / 20% GameServer；
- 某一节点停止 Heartbeat；
- Gateway 节点下线。

观察：

- Registry 收敛时间；
- MMO 已成局 Match 的错误放置或错误路由数量；
- Failover 时间；
- 现有 Session 影响范围。

---

### 实验 D：扩容滞后

场景：

GameServer 从创建到 Ready 人为设置 20~60 秒启动时间。

对比：

- CPU Threshold Scaling；
- Ready Buffer；
- Growth Rate Trigger；
- Predictive Scaling。

观察：

- 首次触发时间；
- Capacity Shortage Duration；
- Queue Length；
- Reject Count。

---

## 5. 初始验收目标（Target，不是承诺值）

这些数值用于指导开发，必须通过真实压测最终确认，不能直接写进简历当“已完成数据”。

### 性能目标

- GameMesh 路由额外 P99 延迟：目标 < 5 ms；
- 常规路由决策：目标 P99 < 2 ms；
- Registry Update 到 Data Plane 可见：目标 < 1 s；
- 节点 Unhealthy 后停止接受新 Match：目标 < 3 s。

### 稳定性目标

- GameServer 宕机不导致 Control Plane 整体不可用；
- 单个策略插件异常不影响 Gateway 核心热路径；
- 扩容期间已有 Match Binding 不被随意迁移；
- 控制状态有 Hysteresis，避免 HIGH/NORMAL 来回抖动。

### 效果目标

在设计的非均匀 GameServer Workload 中，希望相较 Round Robin：

- 明显降低负载离散程度；
- 减少高于安全阈值的节点数量；
- Overload 模式下保持核心流量成功率；
- 扩容准备时间不足时，通过 Admission Control 防止级联故障。

最终效果必须以压测数据为准。

---

## 6. 最小开发环境

第一阶段：

- Go；
- Docker；
- 本地 Fake GameServer；
- Prometheus；
- 可选 Grafana。

第二阶段：

- Kubernetes（kind / k3d / minikube 均可）；
- Agones；
- Prometheus；
- Grafana。

不建议开发第一天就上完整 Kubernetes 集群，否则调试成本会掩盖算法问题。

---

## 7. 项目最终交付物

项目成熟后应包含：

- GameMesh Core；
- Gateway；
- Control Plane；
- GameServer Simulator；
- Load Generator；
- Agones Adapter；
- Kubernetes Manifests / Helm；
- Grafana Dashboard；
- Benchmark 报告；
- Failure Injection 报告；
- Architecture Decision Records；
- Quick Start；
- Demo Video；
- 简历项目描述。

---

## 8. 下一步推荐动作

下一阶段不要立刻写完整 Gateway。

推荐先做：

1. 定义核心领域模型；
2. 定义 GameServer Simulator；
3. 定义 Baseline Round Robin；
4. 定义压测指标；
5. 建立“无 GameMesh / 有 GameMesh”的可重复实验框架；
6. 再进入 M1 编码。

这样从第一天开始，每新增一个策略都可以有数据对比，而不是开发到最后才补压测。
