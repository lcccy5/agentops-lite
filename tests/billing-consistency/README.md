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

### 正常结算与幂等重试

```powershell
pwsh -File ./tests/billing-consistency/verify.ps1 -Count 20
```

每个业务操作发送一次正常请求、一次相同幂等键重试。正常响应固定为 40 Token，重试应返回 409。脚本核验每个操作的实际 Token、账本分录数，再等待 MySQL 账本、Kafka 更新的 MySQL usage_projection 和 Redis consumed 一致，同时检查 reserved=0、active=0。

Count=20 表示 20 个业务操作、40 次请求尝试，不代表 40 个独立计费操作。项目总量包含同一测试环境之前保留的数据；operations.csv 记录本轮独立结果。

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

## 当前范围与后续工作

已提供运行入口：C01 正常结算、C02 顺序幂等重试、C03 Kafka 重复事件（现有 Java IT）、C05 Redis 自动修复。

仍需开发：C02 并发重试、C04 精确崩溃窗口、C06 超时补偿、C07 结算/补偿竞争、正式 P95 采样。当前部署就绪不代表指引中的全部故障场景已实现。

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
