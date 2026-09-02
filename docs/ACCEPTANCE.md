# V0.1 验收

1. `scripts/start-local.ps1` 启动 MySQL、Redis、Kafka、WireMock 与 Toxiproxy。
2. 分别运行 Server、Worker 和指向 Gateway 的基金 Agent。
3. `scripts/demo-online-ledger.ps1` 验证非流式调用、Reservation、Ledger、Outbox 与投影。
4. 使用 `curl -N` 请求 `/v1/chat/completions` 并中途终止，检查 Reservation 最终进入 `CANCELLED` 或 `RECONCILIATION_PENDING` 且 Redis active 归零。
5. `scripts/run-evaluation-release-demo.ps1` 创建版本、导入 12 条数据并运行稳定版与候选版 Job；Worker 必须通过基金 Agent 的 `agent-eval` Endpoint 保存 24 条结果。
6. Gate 分别比较总通过率、工具选择、参数和证据四项，任何一项退化或硬安全失败均拒绝；Gate 还必须绑定同一项目、Prompt Key 和精确版本。
7. 仅通过 Gate 的 Job 可以创建 5% Release；固定 subjectKey 重复解析必须命中同一版本；回滚后新解析不晚于 5 秒恢复稳定版本。
8. `scripts/inject-provider-latency.ps1` 注入延迟，取消客户端后确认上游连接与许可不持续泄漏；最后运行 `scripts/clear-provider-faults.ps1`。
9. `scripts/run-gatling.ps1` 执行 10 条完整 SSE 与 10 条主动取消连接；报告只描述为本机可复现负载。

真实模型验收必须另存模型名、温度、时间和 24 条原始结果；WireMock 自动化结果不得描述为真实 Prompt 质量提升。
