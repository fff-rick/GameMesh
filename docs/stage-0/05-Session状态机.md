# 阶段 0：Session 状态机

## 1. 状态

为了覆盖后续阶段而不在阶段 0 写实现，冻结以下概念状态：

``` text
UNAUTHENTICATED
   | auth success
   v
ACTIVE
   | connection lost
   v
DISCONNECTED_GRACE
   | resume success       | grace expired / terminate
   +----------------> ACTIVE ----------------------+
   |                                               |
   +--------------------------------------------> TERMINATED

ACTIVE -- logout/replaced/shutdown-policy --> TERMINATED
UNAUTHENTICATED -- auth fail/conn close --> TERMINATED
```

  -----------------------------------------------------------------------
  状态                                含义
  ----------------------------------- -----------------------------------
  UNAUTHENTICATED                     Conn 已存在，但尚未形成认证玩家
                                      Session

  ACTIVE                              已认证且存在唯一 current Conn

  DISCONNECTED_GRACE                  阶段 5 使用：Conn 已断开，但
                                      Session 在 Grace Period 内保留

  TERMINATED                          终态；Session 不再可恢复
  -----------------------------------------------------------------------

阶段 2 实现时，在尚未启用断线恢复前，`ACTIVE + connection lost`
可直接进入 `TERMINATED`。阶段 5 开启恢复能力后切换到
`DISCONNECTED_GRACE` 路径。

## 2. 默认重复登录策略

阶段 0 冻结默认策略：**New Login Wins（新登录替换旧登录）**。

流程：

1.  新 Conn 完成鉴权，得到相同 UserID；
2.  若不存在 ACTIVE Session，正常建立；
3.  若存在 ACTIVE Session：
    -   旧 Session/Conn 收到"被新登录替换"的关闭原因；
    -   旧 Conn 进入 CLOSING；
    -   旧 Session 进入 TERMINATED；
    -   新登录创建新的 SessionID 并进入 ACTIVE。
4.  阶段 5 Resume 不视为"新登录"；合法 Resume 应恢复原
    SessionID，并保证最终只有一个 current Conn。
5.  并发重复登录必须通过原子注册/串行化策略得到唯一胜者，不能出现两个稳定
    ACTIVE Session。

## 3. 转换表

  ---------------------------------------------------------------------------------------
  当前                 事件               下一状态             结果
  -------------------- ------------------ -------------------- --------------------------
  UNAUTHENTICATED      auth success       ACTIVE               绑定
                                                               UserID/SessionID/current
                                                               Conn

  UNAUTHENTICATED      auth fail          TERMINATED           不允许业务消息

  UNAUTHENTICATED      conn lost          TERMINATED           清理临时状态

  ACTIVE               logout             TERMINATED           主动结束

  ACTIVE               duplicate new      TERMINATED           旧 Session 被替换
                       login wins                              

  ACTIVE               conn               TERMINATED           未启用 Resume 时直接结束
                       lost（阶段2-4）                         

  ACTIVE               conn               DISCONNECTED_GRACE   保留恢复所需最小状态
                       lost（阶段5+）                          

  DISCONNECTED_GRACE   valid resume       ACTIVE               原 SessionID 绑定新 ConnID

  DISCONNECTED_GRACE   grace expired      TERMINATED           自动回收

  DISCONNECTED_GRACE   invalid/replayed   DISCONNECTED_GRACE   拒绝恢复，不改变合法
                       token                                   Session

  任意非终态           forced terminate   TERMINATED           释放 Session

  TERMINATED           任意事件           TERMINATED           终态、幂等
  ---------------------------------------------------------------------------------------

## 4. Backend 故障

Backend 故障**不改变 ACTIVE Session 的身份状态**。它只导致对应请求按
Backend 错误模型失败。这样可避免 Backend
短暂不可用把客户端会话无谓销毁。

## 5. Gateway Shutdown

阶段 7 之前，Shutdown 的契约结果是：

-   不创建新的 Session；
-   已有 ACTIVE Session 的新业务请求停止接收或明确失败；
-   Conn 最终 CLOSED；
-   单节点且未启用 Resume/Migration 时 Session 最终 TERMINATED；
-   后续多节点/迁移能力不得改变"同一 Session
    最终只有一个有效连接"的不变量。
