# AgentOps Lite V0.1

面向 Java Agent 后端岗位的可复现纵向项目：OpenAI 兼容 SSE 网关、Redis/MySQL/Kafka Token 账本，以及驱动真实基金 Agent 编排的 Prompt 评测门禁与稳定灰度。

## 运行

要求 Java 21+、Maven 3.9+、Docker Desktop。

```powershell
.\scripts\start-local.ps1
.\scripts\run-server.ps1
.\scripts\run-worker.ps1
.\scripts\run-fund-agent-through-gateway.ps1
.\scripts\run-evaluation-release-demo.ps1
```

开发 API Key 为 `agentops-dev-key`，管理 Token 为 `local-admin-token`，仅供本地演示。生产环境必须通过环境变量覆盖。

## 两条证据链

- 在线链：API Key → MySQL PENDING → Redis Lua 预占 → RESERVED → Provider SSE → 独立结算 → 不可变 Ledger/Outbox → Kafka 幂等 Projection。
- 发布链：12 条 Dataset → Eval Outbox → 基金 Agent 本地受控 Endpoint → 工具选择/参数/证据/答案规则评分 → 四维不退化 Gate → SHA-256 5% 灰度 → 5 秒内回滚。
- 恢复链：Kafka 消费重试耗尽后，Worker 重置对应唯一 Outbox 行并重投；`eval_result` 唯一键保证重复投递不产生重复结果。

接口、故障注入和验收步骤见 [docs/ACCEPTANCE.md](docs/ACCEPTANCE.md)，一致性取舍见 [docs/ADR-001-scope-and-consistency.md](docs/ADR-001-scope-and-consistency.md)。

## 诚实边界

WireMock 与 Toxiproxy 证明治理逻辑可复现，不代表生产流量。确定性通道替换外部模型与基金数据，但保留基金 Agent 的编排、工具、证据和安全链。真实模型 Prompt 对比必须单独运行并保存模型名、温度、时间及 24 条原始结果；未执行前不得宣称真实 Prompt 质量提升。V0.1 不包含 UI、K8s、复杂路由和商业计费。
