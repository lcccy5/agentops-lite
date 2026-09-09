#requires -Version 7.0
$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))

Push-Location $projectRoot
try {
    & mvn -pl agentops-worker -am verify `
        '-Dtest=__NoUnitTests__' `
        '-Dsurefire.failIfNoSpecifiedTests=false' `
        '-Dit.test=UsageProjectionIT#compensatesExpiredReservationWithoutLeakingQuota' `
        '-Dfailsafe.failIfNoSpecifiedTests=false'
    if ($LASTEXITCODE -ne 0) { throw 'Expired-reservation compensation test failed.' }

    $reportPath = Join-Path $projectRoot 'agentops-worker/target/failsafe-reports/TEST-io.agentops.lite.worker.UsageProjectionIT.xml'
    $resultLine = Select-String -LiteralPath $reportPath -Pattern 'Expired reservation result:' | Select-Object -Last 1
    if (-not $resultLine) { throw 'Test passed, but its compensation result was not found in the report.' }
    Write-Host ''
    Write-Host "通过：$($resultLine.Line.Trim())" -ForegroundColor Green
} finally {
    Pop-Location
}