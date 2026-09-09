#requires -Version 7.0
$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))

Push-Location $projectRoot
try {
    & mvn -pl agentops-server -am verify `
        '-Dtest=__NoUnitTests__' `
        '-Dsurefire.failIfNoSpecifiedTests=false' `
        '-Dit.test=UsageGatewayIT#rejectsConcurrentIdempotencyRetryWithoutDoubleCharging' `
        '-Dfailsafe.failIfNoSpecifiedTests=false'
    if ($LASTEXITCODE -ne 0) { throw 'Concurrent idempotency test failed.' }

    $reportPath = Join-Path $projectRoot 'agentops-server/target/failsafe-reports/TEST-io.agentops.lite.server.UsageGatewayIT.xml'
    $resultLine = Select-String -LiteralPath $reportPath -Pattern 'Concurrent idempotency result:' | Select-Object -Last 1
    if (-not $resultLine) { throw 'Test passed, but the concurrent HTTP result was not found in its report.' }
    Write-Host ''
    Write-Host "通过：$($resultLine.Line.Trim())" -ForegroundColor Green
} finally {
    Pop-Location
}