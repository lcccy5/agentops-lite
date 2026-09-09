#requires -Version 7.0
$ErrorActionPreference = 'Stop'
$scriptRoot = $PSScriptRoot
$scenarios = @(
    @{ Name = '并发幂等重试'; Script = 'run-concurrent-idempotency.ps1' },
    @{ Name = 'MySQL 事务原子性'; Script = 'run-mysql-atomicity.ps1' },
    @{ Name = 'Outbox Kafka 补发'; Script = 'run-outbox-retry.ps1' },
    @{ Name = 'Kafka 消费写库恢复'; Script = 'run-kafka-consumer-recovery.ps1' },
    @{ Name = 'Kafka 重复消息'; Script = 'run-kafka-duplicate-message.ps1' },
    @{ Name = '预占超时补偿'; Script = 'run-expired-reservation.ps1' },
    @{ Name = '缺少模型调用 ID 的恢复'; Script = 'run-missing-provider-id-recovery.ps1' },
    @{ Name = '恢复任务竞争'; Script = 'run-racing-recovery.ps1' }
)

foreach ($scenario in $scenarios) {
    Write-Host "`n===== $($scenario.Name) =====" -ForegroundColor Cyan
    & pwsh -NoProfile -File (Join-Path $scriptRoot $scenario.Script)
    if ($LASTEXITCODE -ne 0) { throw "$($scenario.Name) 未通过，已停止后续场景。" }
}

Write-Host "`n全部基础一致性测试通过。" -ForegroundColor Green