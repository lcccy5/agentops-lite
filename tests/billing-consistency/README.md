# 计费一致性测试环境：部署与使用

本目录是 billing-consistency-testing-guide.md 的可运行部署入口。基础设施运行在 Docker，Server/Worker 使用 Maven 所用 JDK 在本机后台运行。所有端口只绑定本机回环地址，使用独立 Compose 项目 agentops-billing-test，不依赖原 docker-compose.yml。

## 前置要求

- Windows、PowerShell 7（脚本通过 -NoProxy 绕过本机代理；Windows PowerShell 5.1 不适用）。
- Docker Desktop 已启动并切换 Linux 容器。
- Maven 3.9+，其运行 JDK 为 21+。用 mvn -version 核对，不能只看 java -version。
- 在仓库根目录执行下列命令。

## 启动

```powershell
pwsh -File ./tests/billing-consistency/start.ps1
```

默认构建并运行单元测试，依次启动 MySQL、Redis、Kafka、WireMock、Server、Worker。Server 负责执行 Flyway 迁移，脚本随后将测试数据库中的 Provider 地址改成本机 WireMock，再启动 Worker。

已有当前代码的可执行包时可加 -SkipBuild。启动失败会保留现场与日志；先排查，再执行 stop.ps1 后重试，避免误连占用相同端口的其他服务。

| 服务 | 地址/端口 |
|---|---|
| Server | http://127.0.0.1:28080 |
| Server 健康检查 | http://127.0.0.1:28080/actuator/health |
| Worker 健康检查 | http://127.0.0.1:28082/actuator/health |
| 控制台 | http://127.0.0.1:28080/console |
| MySQL | 127.0.0.1:23306，库 agentops，用户 agentops，密码 billing-test-only |
| Redis | 127.0.0.1:26379 |
| Kafka | 127.0.0.1:29092 |
| WireMock | http://127.0.0.1:28090 |

控制台用户名 admin，密码 billing-test-admin；管理 API 使用 X-AgentOps-Admin-Token: billing-test-admin；网关使用 Bearer agentops-dev-key。上述均为本机测试专用凭据。

## 运行验收

### 基础一致性测试总入口

```powershell
pwsh -File ./tests/billing-consistency/run-baseline-consistency-tests.ps1
```

依次运行并发幂等重试、Kafka 重复消息和预占超时补偿。每一项会打印业务结果；任一项失败后立即停止，避免后续输出掩盖问题。
### 正常结算与幂等重试

```powershell
pwsh -File ./tests/billing-consistency/verify.ps1 -Count 20
```

每个业务操作发送一次正常请求、一次相同幂等键重试。正常响应固定为 40 Token，重试应返回 409。脚本核验每个操作的实际 Token、账本分录数，再等待 MySQL 账本、Kafka 更新的 MySQL usage_projection 和 Redis consumed 一致，同时检查 reserved=0、active=0。

Count=20 表示 20 个业务操作、40 次请求尝试，不代表 40 个独立计费操作。项目总量包含同一测试环境之前保留的数据；operations.csv 记录本轮独立结果。

### 预占超时自动释放

```powershell
pwsh -File ./tests/billing-consistency/run-expired-reservation.ps1
```

测试先模拟一个已经预占 200 Token 和 1 个并发名额、但模型尚未开始执行就已过期的请求；随后执行 Worker 的真实恢复和补偿任务。通过时会显示 `reservation=CANCELLED`、`redisReserved=0`、`redisActive=0`，表示过期请求没有遗留额度占用。
### 已调用模型但缺少调用 ID 的恢复

```powershell
pwsh -File ./tests/billing-consistency/run-missing-provider-id-recovery.ps1
```

模拟请求可能已经到达模型服务，但系统没有留下可查询的模型调用 ID，之后请求过期。Worker 应将其标为 `SETTLEMENT_PENDING`，释放 Redis 并发名额，但保留 Token 预占以等待后续人工或 Provider 对账；通过时会显示 `redisReserved=200`、`redisActive=0`。
### Redis 更新失败与自动恢复

```powershell
pwsh -File ./tests/billing-consistency/repair.ps1 -Samples 3
```

脚本为每个样本临时注册一个延迟 6 秒、固定 40 Token 的 WireMock 响应。在数据库确认已完成准入并开始 Provider 调用后，临时禁止独立测试 Redis 的 EVAL/EVALSHA，使后续结算 Lua 失败；确认 MySQL 账本已提交且 Redis 恰好少 40 Token 后恢复权限，等待 Worker 的持久化任务自动修复。finally 会恢复权限并删除本次 WireMock 映射，不直接修改账本或 Redis 用量来伪造恢复。

必须单独执行：不要同时运行网关流量、其他验收脚本或人工操作这个测试环境。该故障会影响测试 Redis 的全部 Lua 操作。若脚本被强制杀死而未执行 finally，先恢复权限再继续：

```powershell
docker compose -p agentops-billing-test -f ./tests/billing-consistency/compose.yml exec -T redis redis-cli ACL SETUSER default +eval +evalsha
```

本轮记录的是“账本 occurred_at 到观察到 Redis 一致且任务 APPLIED”的耗时。occurred_at 在事务提交前生成，末端包含轮询与 docker exec 开销，因此不是精确的提交到修复延迟。报告明确记录此限制；默认 3 个样本仅用于验证环境和恢复功能，不能直接将 P95 写入简历性能成果。正式采样需要补齐精确事件埋点、时钟校验及更多独立样本。

### 现有 Java 集成测试

```powershell
mvn -B -ntp verify '-Dtest=__NoUnitTests__' '-Dsurefire.failIfNoSpecifiedTests=false' '-Dit.test=UsageGatewayIT,UsageProjectionIT' '-Dfailsafe.failIfNoSpecifiedTests=false'
```

Testcontainers 创建临时依赖并自动清理，不复用这里的数据卷。UsageGatewayIT 验证正常结算、幂等与流式取消；UsageProjectionIT 验证相同事件重复投递后只应用一次。测试夹具已同步当前从 provider_config 读取 Provider 的行为。

## 场景清单与当前结果

| 场景 | 一键入口 | 通过时的核心证据 |
|---|---|---|
| 并发相同幂等键 | `run-concurrent-idempotency.ps1` | 一个 200、一个 409；一条账本 |
| MySQL 事务原子性 | `run-mysql-atomicity.ps1` | Outbox 写入失败时，账本与 Outbox 均为 0 |
| Outbox 发送失败后的补发 | `run-outbox-retry.ps1` | 失败记录留在 PENDING，重试后全部 PUBLISHED |
| Kafka 消费写库失败后的恢复 | `run-kafka-consumer-recovery.ps1` | 回滚幂等标记；重试与重复投递后每笔只汇总一次 |
| Worker 崩溃后的 Kafka 重投模拟 | `run-kafka-duplicate-message.ps1` | `sent=2, applied=1` |
| 未开始模型调用的超时补偿 | `run-expired-reservation.ps1` | `CANCELLED`；预占和并发均为 0 |
| 已开始调用但缺少模型 ID | `run-missing-provider-id-recovery.ps1` | 释放并发；Token 保留为待核验 |
| 恢复任务竞争 | `run-racing-recovery.ps1` | 两次扫描只生成一条补偿任务 |
| Redis 更新失败后的修复 | `repair.ps1 -Samples N` | MySQL 账本与 Redis 再次一致；输出观测 P95 |

`run-kafka-duplicate-message.ps1` 模拟的是 Worker 已完成数据库投影、却在提交 Kafka 位点前崩溃后产生的同一事件重投。它验证重投后的数据结果，不对真实进程杀死时机作性能结论。

正式修复时效采样在启动隔离环境后执行：

```powershell
pwsh -File ./tests/billing-consistency/repair.ps1 -Samples 100
```

该脚本会输出有效样本、未修复样本和“事件到观察到恢复”的 P95。样本达到 100 个不代表自动具备性能结论，仍需结合故障范围、计时误差和测试条件解释。

## 实测记录：100 次 Redis 更新失败后的自动修复

测试日期：2026-09-08。运行 ID：`d066c134-e4e5-4981-8874-23bd80adcfdd`。

本轮验证 MySQL 已记账、Redis 配额更新失败后，Worker 能否通过持久化任务自动恢复一致。同一种故障串行重复注入 100 次，不代表 100 种故障或 100 并发。

### 测试条件与方法

- 环境：本机隔离的 MySQL、Redis、Kafka、WireMock，以及 Server/Worker。
- 模型桩：固定返回 40 Token，响应延迟 6 秒。
- 故障：请求准入后暂时禁止 Redis EVAL/EVALSHA；确认账本已提交且 Redis 用量相差 40 Token，再恢复权限。
- 恢复：由 Worker 的持久化任务执行，恢复扫描周期为 10 秒；不人工修改账本或用量来完成修复。
- 验收：Redis consumed 与账本总 Token 一致，reserved 和 active 均为 0，当前操作的配额任务为 APPLIED。
- 观察窗口：恢复 Redis 权限后最多观察 45 秒；轮询包含 200 毫秒休眠，实际间隔还包括命令执行时间。

### 实测结果

| 指标 | 结果 |
|---|---:|
| 计划 / 实际执行样本数 | 100 / 100 |
| 有效差异样本数 | 100 |
| 自动修复样本数 | 100 |
| 观察窗口内未修复样本数 | 0 |
| 无效样本数 | 0 |
| 观测修复耗时 P50 | 4.448 秒 |
| 观测修复耗时 P95 | 13.417 秒 |
| 最大观测修复耗时 | 15.955 秒 |

统计采用最近秩法：将已修复样本耗时升序排列，P95 取第 ceil(0.95 × n) 个值。本轮 100 个有效样本均修复，未排除未修复样本；已用逐样本 CSV 复核分位数。

### 数据来源与结论边界

- [汇总报告 summary.json](../../artifacts/billing-consistency/d066c134-e4e5-4981-8874-23bd80adcfdd/summary.json)
- [逐样本明细 repairs.csv](../../artifacts/billing-consistency/d066c134-e4e5-4981-8874-23bd80adcfdd/repairs.csv)
- [采样日志 stdout.log](../../artifacts/billing-consistency/measurement-100/stdout.log)

原始产物位于被 Git 忽略的 artifacts 目录，向其他人交付验证证据时需一并提供；本节保存本轮结果摘要。

计时起点使用事务提交前生成的账本 occurred_at，终点为脚本观察到 Redis 一致且任务 APPLIED 的时间。数值包含恢复调度等待、轮询和本机 Docker 命令开销，不是精确的事务提交到 Redis 修复延迟，也不是接口响应时间。

这组数据支持“在本轮故障注入中，系统能自动修复 Redis 用量偏差”的结论。它没有验证真实金额扣费、外部供应商账单对账、高并发恢复能力或所有故障场景；也没有优化前后的同条件对照，因此不据此宣称性能优秀或修复速度显著提升。

面试时可以描述为：在独立环境里重复制造 Redis 更新失败，确认 MySQL 已记账而 Redis 未同步后，观察 Worker 自动恢复；本轮 100 个有效样本均在观察窗口内修复。在 10 秒恢复扫描周期下，观测修复耗时 P95 为 13.42 秒。简历正文可保留设计与验证方式，把样本规模和耗时留在本节作为追问依据。

## 新增跨存储一致性验证：10 条小批量样本

三条测试均先以 `-Dbilling.sample.count=1` 跑通单条故障样本，再使用同一测试逻辑执行 10 条独立事件。10 表示本轮的小批量验证规模，不是吞吐量或线上可用性承诺。

| 场景 | 注入的故障 | 10 条样本结果 |
|---|---|---|
| MySQL 事务原子性 | 在账本写入后，使 Outbox 插入报错 | 10 次失败均回滚；账本残留 0，Outbox 残留 0 |
| MySQL → Kafka | 发送 Kafka 时抛出异常 | 10 条均保留为待发；恢复后 10 条均发布并汇总 400 Token |
| Kafka → MySQL | 投影首次写 MySQL 时失败 | 首条失败回滚；随后 10 条恢复成功，10 次重复投递后总计仍为 400 Token |

以后只要在项目根目录执行以下脚本即可；脚本会从测试报告中提取最后一行结果：

```powershell
pwsh -File ./tests/billing-consistency/run-mysql-atomicity.ps1
pwsh -File ./tests/billing-consistency/run-outbox-retry.ps1
pwsh -File ./tests/billing-consistency/run-kafka-consumer-recovery.ps1
```

这组测试验证的是“失败后正确恢复”的数据完整性，不用于得出 Kafka 延迟、吞吐量或真实宕机恢复时长的性能结论。
## 当前范围与后续工作

已提供运行入口：正常结算、顺序与并发幂等、Kafka 重复事件、MySQL 事务原子性、Outbox 补发、Kafka 消费写库恢复、超时补偿、恢复任务竞争，以及 Redis 自动修复。

仍可继续增强：真实进程在“数据库提交与 Kafka 位点提交之间”被终止的精确崩溃窗口，以及多实例、高并发和生产环境的恢复时效采样。当前结果来自隔离测试环境。

项目已有 Gatling SSE 场景，但它不等于计费正确性压测；本轮不另外安装 k6。未来并发测试应继续复用现有 agentops-test-support 模块，并增加样本级账务核验。

当前实现的 cost_delta 固定为 0。本轮验证 Token 账本、额度与投影，不声称验证人民币/美元扣费或第三方账单对账。Kafka 投影落在 MySQL，Redis 配额由 usage_quota_task 单独修复，二者分别核验。

## 报告与日志

- artifacts/billing-consistency/environment/：进程 PID/启动时间、部署信息、Server/Worker 日志。
- artifacts/billing-consistency/<run-id>/operations.csv：每个操作的幂等和账本结果。
- artifacts/billing-consistency/<run-id>/repairs.csv：故障触发、恢复时间与失败明细。
- 每轮 summary.json：成功、失败及统计口径。verify.ps1 另输出 manifest.json，记录提交与工作区状态。
- agentops-server/target/failsafe-reports/、agentops-worker/target/failsafe-reports/：Java 集成测试报告。

失败报告同样保留，不删除失败样本。产物位于已被 .gitignore 排除的 artifacts/ 下。

## 停止与再启动

```powershell
pwsh -File ./tests/billing-consistency/stop.ps1
```

脚本核对 PID 与进程启动时间，只停止自己记录的 Server/Worker，再停止专用 Compose 项目。MySQL/Redis 数据卷及报告保留。Kafka 容器重建后消息可能不保留，本环境不用于验证 Kafka 数据持久性。

以后再运行 start.ps1 即可启动；服务不会在系统重启后自动启动。不要运行原 scripts/stop-demo.ps1 管理这个环境。
