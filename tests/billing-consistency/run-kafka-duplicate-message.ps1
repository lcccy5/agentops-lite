#requires -Version 7.0
$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))

Push-Location $projectRoot
try {
    & mvn -pl agentops-worker -am verify `
        '-Dtest=__NoUnitTests__' `
        '-Dsurefire.failIfNoSpecifiedTests=false' `
        '-Dit.test=UsageProjectionIT#appliesRedeliveredLedgerEventExactlyOnce' `
        '-Dfailsafe.failIfNoSpecifiedTests=false'
    if ($LASTEXITCODE -ne 0) { throw 'Kafka duplicate-message test failed.' }

    $reportPath = Join-Path $projectRoot 'agentops-worker/target/failsafe-reports/TEST-io.agentops.lite.worker.UsageProjectionIT.xml'
    $resultLine = Select-String -LiteralPath $reportPath -Pattern 'Kafka duplicate-message result:' | Select-Object -Last 1
    if (-not $resultLine) { throw 'Test passed, but its Kafka result was not found in the report.' }
    Write-Host ''
    Write-Host "通过：$($resultLine.Line.Trim())" -ForegroundColor Green
} finally {
    Pop-Location
}