# 05. 调研来源

> 调研日期：2026-08-21。优先使用官方文档与官方代码仓库。

## 1. Agones

### Agones Documentation

- https://agones.dev/site/docs/
- 结论：Agones 是基于 Kubernetes 的 Dedicated Game Server 托管、运行和扩缩容方案；本轮官方文档显示 Release 1.60.0。

### GameServer

- https://agones.dev/site/docs/reference/gameserver/
- 结论：提供 GameServer CRD、端口、健康检查等生命周期能力。

### Fleet Autoscaler

- https://agones.dev/site/docs/reference/fleetautoscaler/
- 结论：支持 Ready Buffer、Webhook、Wasm、Schedule、Chain 等扩缩容方式。

### GameServerAllocation

- https://www.agones.dev/site/docs/reference/gameserverallocation/
- 结论：支持从候选 GameServer 中原子分配实例；部分查询缓存为 eventual consistency。

### Allocator Service

- https://www.agones.dev/site/docs/advanced/allocator-service/
- 结论：提供 mTLS 的 gRPC / REST Allocation 服务，适合 GameMesh Control Plane 作为外部调度器接入。

### Player Capacity Integration Pattern

- https://agones.dev/site/docs/integration-patterns/player-capacity/
- 结论：可利用 Counters / Lists 表达玩家容量；相关能力仍需注意 Beta 与 eventual consistency 边界。

### Matchmaker Integration Pattern

- https://agones.dev/site/docs/integration-patterns/allocation-from-fleet/
- 结论：官方推荐外部 Matchmaker 从 Fleet 请求 GameServer Allocation，这与 GameMesh Scheduler 架构高度吻合。

---

## 2. Kubernetes

### Horizontal Pod Autoscaling

- https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/
- 结论：HPA 支持 CPU、Memory 与 Custom Metrics；其控制器周期性运行，官方文档给出的默认 sync period 为 15 秒。

### Autoscaling API

- https://kubernetes.io/docs/reference/kubernetes-api/autoscaling/
- 结论：HPA 可对实现 scale subresource 的 Workload 管理 replica count。

---

## 3. Envoy

### Load Balancing

- https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/load_balancing/load_balancing
- 结论：Envoy 支持 Weighted Round Robin、Least Request、Ring Hash、Maglev、Random 等多种通用负载均衡策略。

### Supported Load Balancers

- https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/load_balancing/load_balancers.html
- 结论：Least Request / P2C 与 Ring Hash 等成熟算法可以作为 GameMesh Baseline 与算法参考。

### Circuit Breaking

- https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/circuit_breaking
- 结论：通用分布式系统中应快速失败并施加 Backpressure，避免压力继续向下游传播。

### Overload Manager

- https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/operations/overload_manager.html
- 结论：Envoy 可以根据 CPU、Memory、File Descriptor 等资源压力触发过载动作。GameMesh 可借鉴这一“资源 -> Trigger -> Action”模型，但加入游戏业务指标。

---

## 4. Open Match

### Overview

- https://open-match.dev/site/docs/overview/
- 结论：Open Match 用于构建可扩展、可定制的 Matchmaker，解决 Ticket / Match Generation 等通用问题。

### Releases

- https://github.com/googleforgames/open-match/releases
- 结论：公开 release 页面可见最新条目 v1.8.1，并注明测试 Kubernetes 1.24 / 1.25、修改核心需要 Go >= 1.21。由于版本兼容信息相对保守，建议 GameMesh 通过 Adapter 可选接入，而不是 MVP 硬绑定。

---

## 5. HashiCorp go-plugin

### Repository / README

- https://github.com/hashicorp/go-plugin
- 结论：通过独立进程 + net/rpc 或 gRPC 实现插件，广泛用于 HashiCorp 工具；插件崩溃与宿主隔离，但跨进程通信有成本。

### Plugin Architecture

- https://github.com/hashicorp/go-plugin/blob/main/plugin.go
- 结论：支持 net/rpc 与 gRPC 插件接口。

### Extensive Tutorial

- https://github.com/hashicorp/go-plugin/blob/main/docs/extensive-go-plugin-tutorial.md
- 结论：适合作为 Control Plane 动态插件机制的参考，不建议直接放入每条消息都要经过的 Data Plane 极热路径。

---

## 6. 本轮调研后的架构决策

1. **Kubernetes：复用。**
2. **Agones：核心集成对象。**
3. **Prometheus：复用。**
4. **Open Match：可选 Adapter。**
5. **Envoy：算法与过载设计参考，高级阶段可接入。**
6. **Data Plane Gateway：MVP 自研。**
7. **Game-aware Router / Balancer：自研。**
8. **Adaptive Controller：自研。**
9. **Autoscaling Decision：自研策略，调用现有编排系统执行。**
10. **动态插件：优先放 Control Plane，避免污染热路径。**

