#requires -Version 7.0
$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))

Push-Location $projectRoot
try {
    & mvn -pl agentops-server -am verify `
        '-Dtest=__NoUnitTests__' `
        '-Dsurefire.failIfNoSpecifiedTests=false' `
        '-Dit.test=UsageGatewayIT#rollsBackLedgerAndOutboxTogetherWhenOutboxInsertFails' `
        '-Dfailsafe.failIfNoSpecifiedTests=false' `
        '-Dbilling.sample.count=10'
    if ($LASTEXITCODE -ne 0) { throw 'MySQL 原子性测试失败。' }

    $reportPath = Join-Path $projectRoot 'agentops-server/target/failsafe-reports/TEST-io.agentops.lite.server.UsageGatewayIT.xml'
    $resultLine = Select-String -LiteralPath $reportPath -Pattern 'MySQL atomicity result:' | Select-Object -Last 1
    if (-not $resultLine) { throw '测试通过，但没有找到 MySQL 原子性结果。' }
    Write-Host "`n通过：$($resultLine.Line.Trim())" -ForegroundColor Green
} finally { Pop-Location }