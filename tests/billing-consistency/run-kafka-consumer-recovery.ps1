#requires -Version 7.0
$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))

Push-Location $projectRoot
try {
    & mvn -pl agentops-worker -am verify `
        '-Dtest=__NoUnitTests__' `
        '-Dsurefire.failIfNoSpecifiedTests=false' `
        '-Dit.test=UsageProjectionIT#retriesKafkaConsumptionAfterMySqlProjectionFailure' `
        '-Dfailsafe.failIfNoSpecifiedTests=false' `
        '-Dbilling.sample.count=10'
    if ($LASTEXITCODE -ne 0) { throw 'Kafka 消费恢复测试失败。' }

    $reportPath = Join-Path $projectRoot 'agentops-worker/target/failsafe-reports/TEST-io.agentops.lite.worker.UsageProjectionIT.xml'
    $resultLine = Select-String -LiteralPath $reportPath -Pattern 'Kafka consumer recovery result:' | Select-Object -Last 1
    if (-not $resultLine) { throw '测试通过，但没有找到 Kafka 消费恢复结果。' }
    Write-Host "`n通过：$($resultLine.Line.Trim())" -ForegroundColor Green
} finally { Pop-Location }