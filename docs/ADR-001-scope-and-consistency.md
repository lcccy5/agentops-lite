# ADR-001：V0.1 范围与一致性边界

状态：已采纳，2026-08-31。

AgentOps Lite 只启动 Server 与 Worker 两个进程。MySQL 是 Reservation、Ledger、Prompt、Eval 与 Release 的事实源；Redis 仅负责在线原子额度和许可；Kafka 只负责可恢复的异步投影与评测分发。

在线调用使用 MySQL `PENDING` → Redis Lua → MySQL `RESERVED` 的次序，因而 Redis 成功后的崩溃总能从数据库找到补偿对象。账本不可修改，估算修正必须追加 `USAGE_ADJUSTMENT`。取消响应链不得取消独立结算任务。若实际端点声明 `QUERYABLE` 并返回 generation ID，Worker 在限定期限内查询最终 usage；否则按已捕获的语义内容估算。等待查询仅释放 Redis 并发许可，持久 quota task 负责最终额度变更的重试。

V0.1 不建设 UI、Kubernetes、动态模型路由、商业支付、LLM-as-Judge 或自动回滚。
