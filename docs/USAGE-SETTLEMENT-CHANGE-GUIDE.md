# 双模式用量结算改动指引

日期：2026-09-07。本文保留最初的源码核对和设计决策，已由后续实现落地；实际配置、状态口径与运行方式请以 [USAGE-SETTLEMENT-IMPLEMENTATION.md](USAGE-SETTLEMENT-IMPLEMENTATION.md) 为准。

## 1. 范围与决定

保留客户端取消向上游传播的行为。最终 Provider usage 有效时优先使用；缺失时，按实际调用端点的能力分流：

| 模式 | 缺失最终 usage 后的处理 | 终态 |
| --- | --- | --- |
| ESTIMATE_FALLBACK | 按版本化规则估算已捕获的有效内容，记估算账 | FINAL + ESTIMATED |
| QUERYABLE | 保存可查询的生成 ID，进入待确认并异步查询 | 查到后 FINAL + PROVIDER_QUERY |
| QUERYABLE 超过查询期限 | 按调用开始时锁定的兜底规则估算；不能无限占用额度 | FINAL + ESTIMATED，保留超期原因 |

第一版建议查询到期后结束自动追扣；迟到证据保留，后续更正走有证据、有幂等键的管理流程。查询期限应由具体端点的数据延迟和产品规则确定，不采用全供应商通用的随意秒数。

当前项目结算的是 Token 配额，不是商业金额：`UsageService` 两种入账均把 `cost_delta` 写成 0，`UsageLedgerEvent` 中费用也为零。本次先实现可信的用量与额度闭环；人民币/美元计费、价格快照及支付属于后续独立范围。不能在发布说明里声称已经实现准确金额扣费。

## 2. 已核实的源码现状

下列路径均相对项目根目录；行号基于上述 HEAD，实施时以方法名再定位。

| 文件与位置 | 当前行为 | 改动原因 |
| --- | --- | --- |
| `agentops-server/src/main/java/io/agentops/lite/server/gateway/ChatCompletionsGateway.java:79`，ordinary | 收到完整响应后，要等下游写成功才结算；无独立取消结算分支 | 下游失败不能丢掉已经拿到的真实 usage |
| 同文件 `:110`，stream | 捕获原始字节，doFinally 中独立提交结算；取消计为熔断失败 | 保留独立结算；客户端取消与上游失败分开 |
| 同文件 `:125` | 只在累计大小不超过 2MB 时保存一个 chunk | 超限后可能漏掉尾部 usage，甚至形成不连续捕获内容 |
| 同文件 `:163`，usageFromJson | 只排除 MissingNode，null 或缺字段可能按 0 处理 | 无效 usage 不能成为“真实零用量” |
| 同文件 `:174`，usageFromSse | 返回第一个非空 usage；缺失时按原始 SSE 长度估算 | 阶段 usage 不一定是最终值；协议字节不等于模型输出 |
| 同文件 `:190`，fallbackUsage | partial 输出取 1，随后取最小值 | 中断无 usage 的输出估算固定落到 1 |
| `agentops-core/src/main/java/io/agentops/lite/core/domain/TokenEstimator.java:10` | reserve 对输入、tools、最大输出和安全余量估算 | 这是准入预占，不应直接用作最终输出估算 |
| `agentops-server/src/main/java/io/agentops/lite/server/usage/UsageService.java:98` | 锁 Reservation，追加 Ledger/Outbox，提交后调用 Redis FINALIZE | 保留 MySQL 事实源，但增加持久的 Redis 应用任务 |
| 同文件 `:111`、`:118` | 估算和 Redis 失败都写 RECONCILIATION_PENDING | 计量状态与缓存同步状态混杂 |
| 同文件 `:166`，adjustEstimatedUsage | 差额相对第一条估算流水；只更新 MySQL/Outbox | 扩展多次修正后会算错；Redis 不跟随修正 |
| 同文件 queryRun、queryRecentRuns、isFinal | 非 PENDING/RESERVED 都被看作结束 | 不能表达“调用结束但仍在查用量” |
| `agentops-worker/src/main/java/io/agentops/lite/worker/UsageWorker.java:60` | 消费事件只更新 MySQL usage_projection | 不能在这里直接再给 Redis 加所有 Token，否则正常结算重复计数 |
| 同文件 `:77`，reconcileUsage | 过期先释放 Redis，再修改数据库；已发起上游的请求挂待对账 | 与正常结算存在竞争；没有 Provider 查询或持久估算证据 |
| `agentops-server/src/main/resources/lua/finalize.lua:1` | 一次性释放 reserved、active，并增加 consumed；终态 marker TTL 为 5 分钟 | 无法直接支持“先释放并发、稍后结算额度”及长时间重试 |
| `agentops-server/src/main/resources/db/migration/V1__initial_schema.sql:13` | 有 provider_config 表 | 网关实际没使用这张表选择上游 |
| `agentops-server/src/main/java/io/agentops/lite/server/config/RuntimeConfiguration.java:17` | WebClient 使用全局 providerBaseUrl；认证来自全局 properties | 给 provider_config 加字段并不会自动生效 |

还核对了 `UsageModels.java`、`Contracts.java`、`UsageQueryController.java`、`ApiKeyAuthenticationFilter.java`、`OperatorConsoleController.java`、Server/Worker 配置与 Maven 依赖、两份 Usage IT，以及现有 ADR 和验收文档。未发现适用的 AGENTS.md。

## 3. 第一批改动：配置、领域对象与迁移

### 配置先接当前单上游，不顺带开发动态路由

修改 `agentops-server/src/main/java/io/agentops/lite/server/config/AgentOpsProperties.java`、同目录 `RuntimeConfiguration.java` 和 `agentops-server/src/main/resources/application.yml`。

新增明确的端点身份与结算配置：providerEndpointId、settlementMode、usageLookupAdapter、queryDeadline、queryRetryLimit、estimatorVersion、supportsStreamCancellation。按模型覆盖时，以“实际配置端点 + 实际模型”匹配规则；未识别模型只能使用已声明适用的通用估算规则，否则准入时拒绝。

QUERYABLE 必须绑定已实现的查询适配器，且 Worker 能取得对应凭据。只填布尔开关不算实现查询。模型/供应商未确认前，默认关闭 QUERYABLE。查询地址由可信配置决定，不能使用上游响应中的任意 URL；保存凭据引用，不把 API Key 写进账务表。

Worker 需要新增自己的计量查询配置及 WebClient bean，修改 `agentops-worker/src/main/resources/application.yml`。Worker 已有 WebFlux 依赖，不需要依赖 Server 模块。部署配置和 `.env.example` 同步说明变量；如通过 compose 注入，也同步 `docker-compose.yml`。

### 领域模型

修改 `agentops-core/src/main/java/io/agentops/lite/core/domain/UsageModels.java`。以 UsageEvidence 替代只能表达 boolean estimated 的 ConfirmedUsage，记录输入/输出分项、来源、是否最终、估算版本、证据完整性、端点和查询 ID。未知分项使用未知值，不填 0；0 只表示明确计量为零。

保留原 Reservation.status 处理准入及执行生命周期；增加 settlementStatus（PENDING / FINAL）、usageSource（PROVIDER_STREAM / PROVIDER_QUERY / ESTIMATED / MANUAL）、quotaSyncStatus。取消请求可以同时是执行 CANCELLED、结算 FINAL，不能为了结算将执行结果改成成功。

RECONCILIATION_PENDING 仅作旧数据兼容，不再作为新流程的多用途状态。估算但不再查询的请求必须可终结。

### 数据库迁移

当前最新迁移是 V5。新增 `agentops-server/src/main/resources/db/migration/V6__add_usage_settlement_tracking.sql`，实施时若版本已被占用则顺延；不要改 V1/V4 已发布迁移。

建议最小结构：

| 对象 | 新增字段或约束 |
| --- | --- |
| usage_reservation | settlement_status、execution_outcome、usage_source 扩展、input_tokens、output_tokens、settlement_version、settlement_deadline、quota_sync_status、策略快照及 estimator_version |
| usage_provider_attempt（新表） | attempt_id、reservation_id、attempt_no、provider_endpoint_id、requested_model、actual_model、provider_trace_id、provider_generation_id、provider_started_at、最后证据/捕获完整性；唯一 reservation_id + attempt_no |
| usage_lookup_task（新表） | attempt_id 唯一、status、next_attempt_at、attempts、deadline、lease_owner、lease_until、lease_version、last_error_code；按 status + next_attempt_at 索引 |
| usage_ledger | settlement_version / operation_id 唯一幂等约束，来源与证据引用；保留原流水不可变 |
| usage_quota_task（新表） | reservation_id、quota_version、目标 active/held/charged 值、状态、重试时间及租约；唯一 reservation_id + quota_version |

原 `uk_ledger_reservation_type` 含 nullable related_ledger_id，不能将它视为所有初次入账重复的绝对保障。新业务操作幂等键必须非空；历史回填用各自 ledger_id 形成唯一标识。

actual_tokens 历史上包含估算值。兼容保留该字段，但新接口使用 settledTokens 配合 source 表达；精确的 input/output 不能从历史总数反推。

历史 RECONCILIATION_PENDING 需按 Ledger、usage_source、failure_code 分类：已真实入账但 Redis 失败的只补 Redis；有估算账但无可查 ID 的保留估算来源；已启动上游但无账本的进入异常处理。不要凭旧状态自动产生新扣款，也不要假造 Provider ID。

## 4. 第二批改动：网关捕获与估算

修改 `ChatCompletionsGateway` 的 ordinary、stream、readOrdinary、readStream、usageFromJson、usageFromSse、fallbackUsage、finishAsync。

1. reserve 前确定实际端点策略，保存策略快照。建立 attempt；只有确实进入上游调用阶段才标记 started。
2. 从响应头读取 trace ID，从完整 JSON 或 SSE 事件读取可查询 generation ID，两者分开保存。拿到 ID 就持久化，不等 doFinally。把本地 request ID 返回响应头，便于没有收到首个事件时查询本地状态。
3. 新增 `agentops-server/src/main/java/io/agentops/lite/server/gateway/ProviderUsageAccumulator.java`：按完整 SSE 事件处理，兼容 UTF-8、行、事件跨网络 chunk，CRLF、空行和心跳；按适配协议处理累计 usage，不能简单求和或取第一个值。
4. 元数据/usage 解析覆盖整个流，不受诊断原文 2MB 上限影响。日志原文可以截断，计量不能默默截断。记录截断、解析失败、不可支持内容的原因。
5. 只计量语义输出：content，以及规则明确覆盖的 tool_calls 参数等；按 choice/tool 索引归并。隐藏推理、图片、音频等超出能力时，按已配置策略限制准入或进入待核验，不编造 Token。
6. 完整 Provider usage 要校验非 null、必需字段、整数非负和协议上的最终性。收到部分 usage 可保留分项证据，不应强制把缺失分项补零。
7. ordinary 在收到上游完整响应时先捕获 usage，再写下游；下游写失败时仍沿用已捕获证据。为 ordinary 也建立统一、幂等的结束入口。
8. 客户端取消记录为执行取消，不直接向 Provider 熔断器报告服务故障；按 Resilience4j 的调用许可生命周期实现取消处理，确保半开许可不泄漏。
9. JDBC/Redis 持久化不要直接放在 Netty doOnNext/doOnSubscribe 上。通过现有 blockingScheduler 串行处理关键证据写入，维持背压和有界队列。

新增 `agentops-core/src/main/java/io/agentops/lite/core/domain/UsageEstimator.java`，把准入 TokenEstimator 与结算估算分开。第一版可以使用明确命名的启发式 v1，或选定并固定版本的 tokenizer；不能把字符数算法称为精确 tokenizer。

要求：输入、工具结构、可见输出的算法有版本；输出不再使用 partial=1 或 max_tokens/2；不按 SSE 包装长度计量；改变网络分块不改变同一完整内容的估算结果。使用 tokenizer 时不能独立分词每个 chunk 后相加，应保存可重放文本或使用有跨块状态的计数器。

持久证据与费用口径必须一致：若承诺“网关已捕获内容均可计费”，需在相应输出转发前保存可恢复证据；若采用周期检查点，应明确只计持久部分及可能损耗，不能宣称进程崩溃后能还原所有输出。摘要只能验证内容，不能单独重算 Token；估算规则需要的内容/统计量也要保存，并规定保留期。

现有 finishAsync 的内存 submit 不具备持久性。保留独立于取消信号的最终处理，但 Provider ID、最新证据、未完成状态先可恢复；记录提交拒绝和任务异常，Worker 扫描遗漏终结任务。对“上游已接受、ID 尚未返回就崩溃”明确标记 ID_UNAVAILABLE，不盲目重发推理请求。

## 5. 第三批改动：统一结算与后台查询

Server 和 Worker 都依赖 core；Worker 不能直接调用 Server 的 UsageService。建议新增 `agentops-core/src/main/java/io/agentops/lite/core/usage/UsageSettlementService.java`，承接共享的事务化账务操作，使用领域异常，不引用 GatewayException。

`UsageService` 保留准入、HTTP 查询及管理入口，把 finalizeReservation 和 adjustEstimatedUsage 的账务部分委托共享服务。不要复制两份结算 SQL。

统一决策：

| 输入证据 | 动作 |
| --- | --- |
| 已确认未执行/明确不计费拒绝 | 记零用量依据并释放许可与预占；不能把任何 HTTP 5xx 都先验当作免费 |
| 有完整最终 usage | 行锁/CAS 下初次结算，追加 Ledger 和 usage_outbox，创建 quota task |
| 无最终 usage，ESTIMATE_FALLBACK | 使用持久证据估算，写 USAGE_ESTIMATED，结算 FINAL |
| 无最终 usage，QUERYABLE，ID 已有 | 同事务设结算 PENDING、创建唯一 lookup task；释放并发，暂保留额度预占 |
| QUERYABLE 但暂时没有 ID | 明确 ID_UNAVAILABLE，走有期限恢复/估算流程；不能提交注定失败的空 ID 查询 |
| 查询期限耗尽 | 按已锁定兜底规则结束结算，保留超期/缺证据标记，不无限冻结 |

新增 `agentops-worker/src/main/java/io/agentops/lite/worker/ProviderUsageLookupWorker.java`。适配器定义可放 core，具体实现按实际 Provider 协议编写，至少区分 FINAL、NOT_READY、RETRYABLE_ERROR、UNSUPPORTED、INVALID_EVIDENCE；不能把“接口 HTTP 200”直接视为最终 usage。

Worker 使用数据库租约领取任务，带 lease_version 防止过期执行者覆盖新结果；HTTP 查询在事务外，有独立连接/响应超时、并发上限、退避抖动与限次。404 是否可重试由端点契约和时间窗决定；429 尊重重试建议；认证失败告警，不能悄悄标成模型不支持。

查询结果和流式最终证据到达同一请求时，共享结算服务按版本和操作幂等键只结算一次；相同证据重放无新账，不同最终证据进入差异处理，不允许最后写入者静默覆盖。

修正 Token 差额 = 目标确认总量 − 当前有效 Ledger 累计总量，不能只减第一条估算流水。管理修正增加 operationId、reason、evidenceReference、expectedVersion 和操作人记录，维持项目范围及管理员权限。自动核验使用 PROVIDER_QUERY，人工使用 MANUAL，不统统写 ADJUSTED。即使差额为 0，也应更新证据来源/最终性并留下审计操作。

## 6. 第四批改动：Redis 额度闭环（不能跳过）

现有 reserve.lua 同时检查 consumed + reserved 和 active；finalize.lua 同时释放两者。等待查询时必须只释放 active，保留 held tokens 直到结算或查询到期，否则用户可用大量断线请求绕开额度。

本项目建议保留正常路径快速同步 Redis，同时让持久 quota task 负责重试。不要一边保留旧 FINALIZE 增加 consumed，一边在 applyUsageProjection 再对同一 Ledger 全量增加 consumed。

建议将 quota 变更统一为“每个 Reservation 的版本化目标值”：

| 阶段 | active contribution | held tokens | charged tokens |
| --- | --- | --- | --- |
| 准入成功 | 1 | reserved_tokens | 0 |
| 调用结束，等待核验 | 0 | reserved_tokens | 0 |
| 已结算 | 0 | 0 | 当前累计入账总量 |
| 已确认未执行并释放 | 0 | 0 | 0 |

新增共享 Lua，例如 `agentops-core/src/main/resources/lua/apply_usage_quota.lua`，Server/Worker 使用同一份资源。Lua 对比 marker 中已应用版本及 active/held/charged 贡献，原子调整项目 quota 的差值，然后写新版本；旧版本重放不生效。任务携带完整目标，因此版本跳跃或乱序也不会遗漏前序增量。

修改 Server reserve.lua 的 marker 初始化以记录初始贡献；逐步替换 finalize.lua，并同步 Server/Worker 两份 compensate.lua 的行为或抽到 core。补偿、查询释放并发和最终结算必须经过同一版本机制，不能各自直接减计数。

账务事务只写 MySQL 和 quota task，提交后调用 Lua；成功再标 task 已应用。若 Lua 成功但数据库确认失败，重试同版本无副作用。任何同步失败只标 quotaSyncStatus，不覆盖 usageSource/settlementStatus。

现有终结 marker 的 5 分钟 TTL 不适合查询与重试。未完成同步及允许修正期间保留贡献记录；清理需要覆盖消息/任务保留窗口和迟到处理，并有已归档判定。Redis 丢失 marker 时不能假设先前未扣款再加一次。

Redis 全量丢失的恢复：暂停受影响项目准入及 quota task 应用，用 MySQL 一致快照重建 consumed、仍保留的 held 和有效 active 贡献/marker 及版本，再恢复任务与准入。不能在线直接用 Ledger 总和覆盖 consumed 而忽略并发入账。此恢复应有项目级隔离/维护机制及测试。

`UsageWorker.applyUsageProjection` 保留现有 MySQL 去重投影职责。新增 quota task 扫描器，不复用 usage_projection_applied 来假装 MySQL 与 Redis 跨系统事务已经原子提交。

## 7. 第五批改动：过期恢复、接口与展示

修改 UsageWorker.reconcileUsage：分开处理准入未完成、执行失联、结算待查询和 Redis 同步失败。先行锁/CAS 确定状态、创建持久 quota task，再应用 Redis，不能继续“先 Redis 补偿，后争抢 MySQL 状态”。

当前 reservation-timeout=2m 是固定时限。执行租约/心跳、查询截止时间、Redis marker 保留期必须分开；不能把一个合法长响应在 2 分钟时当作已结束释放许可，也不能在查询期间让旧扫描器再次释放已释放的 active。

过期但 provider_started 的请求使用保存的 attempt 和证据分流；不再只留下一个没有 Ledger/查询任务的 RECONCILIATION_PENDING。已正常结算的请求只补缓存，不生成新账。

修改 `UsageService.queryRequest/queryRun/queryRecentRuns/querySummary`：分别返回执行结果、结算状态、用量来源、settledTokens、已确认/估算/待确认统计以及同步状态。run 是否最终结算依据 settlementStatus，不能再用 isFinal 的“非 PENDING/RESERVED”判断。

修改 `agentops-server/src/main/java/io/agentops/lite/server/usage/UsageQueryController.java`：保持现有 queryRequest/queryRun/adjustEstimatedRequest 路径兼容，扩展修正请求。现有类注释称 Read-only，但包含修改接口，实施时同步纠正。

现有接口均在 `/internal/` 下，由管理员 token 保护；不是普通用户账单 API。若需要对外查询，新增独立的 `GET /v1/usage/queryRequest/{requestId}`，通过现有 Bearer API Key 得到 projectId 并在 SQL 中强制隔离；不能把管理员凭据发给用户。新增写接口遵循已有动作式路径命名。

修改 `agentops-server/src/main/java/io/agentops/lite/server/console/OperatorConsoleController.java`：显示“待核验/供应商确认/估算结算”，保留执行取消状态，不把估算 total 标成已确认实际用量。

更新 `docs/ACCEPTANCE.md`、`docs/ADR-001-scope-and-consistency.md` 和 README。ADR 仍写 V0.1 不建设 UI，但源码已有轻量 console，应注明当前事实；本改动不引入商业支付或动态路由。

## 8. 实施顺序与验收

按配置/迁移 → 证据捕获 → 共享结算与 quota 版本机制 → 查询 Worker/恢复 → 接口展示顺序拆分。发布时先停旧 Worker 的过期扫描并排空在途旧结算，迁移和对齐 quota 贡献后再切新写路径；不能让旧 Lua 与新版本 Lua 同时处理同一请求。数据库迁移保留向后兼容，关闭查询开关不等于可以回滚到会误处理新状态的旧 Worker。

已有测试应扩展，而不是仅新增镜像实现的单测：

| 测试位置 | 必须增加的可观察断言 |
| --- | --- |
| `agentops-server/src/test/java/io/agentops/lite/server/UsageGatewayIT.java` | 估算渠道取消后输出随内容增加、不是固定 1；active=0、reserved=0、consumed=Ledger；账单 source=ESTIMATED 且结算 FINAL |
| 同上 | 可查询渠道取消时 ID 已持久化，active=0、reserved 保留、consumed 未增加、唯一 lookup task |
| 同上 | 尾部 usage 在 2MB 后仍识别；UTF-8/SSE 任意分块不改变计量；null/缺字段/阶段 usage 不当最终数据 |
| 同上 | ordinary 已捕获真实 usage 后下游写失败仍按真实值结算；取消不会触发无依据零账或 max_tokens/2 |
| 新增 Worker 查询 IT | 先 NOT_READY 后 FINAL、查询超期估算、缺 ID、租约过期双 Worker、重复结果、流式与查询竞争，每次只产生一次有效结算 |
| `agentops-worker/src/test/java/io/agentops/lite/worker/UsageProjectionIT.java` | Kafka 重投保持 MySQL 投影幂等；不重复增加 Redis |
| 新增 quota/recovery IT | 100→160→80 后三处总数都是 80；任务乱序/重复、Redis 成功 DB 确认失败、marker 丢失、全量重建都无双扣或双释放 |
| 新增 recovery IT | 正常结算与到期扫描竞争；执行租约续期；进程崩溃后凭持久 ID 恢复；证据不足明确标记 |
| core 新增估算测试 | 混合中英文、工具参数、空输出、多 choice、不同分块；同证据同版本可复算 |
| 查询/管理接口测试 | 跨项目读取/调整拒绝；过期 expectedVersion 拒绝；同 operationId 重复提交返回同结果 |
| 迁移测试 | 历史三类 RECONCILIATION_PENDING 分类正确，真实用量 Redis 失败不能误做估算修正 |

单测可用根目录 `mvn test`；完整验收使用已有 `scripts/verify.ps1`（内部执行 `mvn clean verify`，包括 Docker-backed Failsafe IT，需要 Java 21、Maven 和 Docker）。本次仅写指引，未运行上述业务测试，不代表实现已通过。

上线指标至少包含 lookup 待处理数/年龄/失败原因、估算比例、证据截断数、未确认预占量、quota task 延迟、Ledger/Redis 差异。查询开关只对已验证支持取消后请求级最终用量的端点开启。

## 9. 发布前必须定下的产品规则

无查询能力时采用哪一版估算算法、哪些模型/内容类型可用；查询多久后转估算；估算最终结算后是否允许人工修正以及期限；是否承诺费用上限。若承诺上限，要单独记录 Provider 实际用量与用户计入配额值，不能偷偷截断真实 usage 来满足上限。

对外描述应是：“正常按供应商确认用量结算；缺失用量时，支持查询的渠道限期核验，不支持或超期的渠道按已公布规则估算，记录来源及原因。”它避免“中断就免费”，但不保证估算等于上游成本，也不保证取消立即停止供应商计费。
