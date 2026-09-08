param([ValidateRange(1,1000)][int]$Count=20)
. "$PSScriptRoot/common.ps1"
$runId = [guid]::NewGuid().ToString()
$reportDir = Join-Path $RepoRoot "artifacts/billing-consistency/$runId"
New-Item -ItemType Directory -Force $reportDir | Out-Null
$admin = @{'X-AgentOps-Admin-Token'='billing-test-admin'}
$rows = [Collections.Generic.List[object]]::new()
$summary = [ordered]@{runId=$runId;result='FAIL';uniqueOperations=$Count;requestAttempts=0;expectedTokensPerOperation=40;coverage=@('C01 normal settlement','C02 sequential idempotency retry');notCovered=@('C02 concurrent retry','C03 duplicate Kafka','C04 crash window','C06 timeout compensation','C07 settlement race');error=$null}
try {
    Wait-BillingHealth 'http://127.0.0.1:28080/actuator/health'
    foreach ($index in 1..$Count) {
        $id = [guid]::NewGuid().ToString()
        $headers = @{Authorization='Bearer agentops-dev-key';'Idempotency-Key'=$id;'X-AgentOps-Request-Id'=$id;'X-AgentOps-Correlation-Id'=$id}
        $body = @{model='deterministic-fund-model';stream=$false;max_tokens=128;messages=@(@{role='user';content='billing consistency baseline'})} | ConvertTo-Json -Depth 5
        $summary.requestAttempts++
        $response = Invoke-RestMethod -NoProxy -Uri 'http://127.0.0.1:28080/v1/chat/completions' -Method Post -Headers $headers -ContentType 'application/json' -Body $body
        if ($response.usage.total_tokens -ne 40) { throw 'Model fixture usage differs from expected 40.' }
        $summary.requestAttempts++
        $duplicateStatus = 0
        try { Invoke-RestMethod -NoProxy -Uri 'http://127.0.0.1:28080/v1/chat/completions' -Method Post -Headers $headers -ContentType 'application/json' -Body $body | Out-Null } catch {
            if ($_.Exception.Response) { $duplicateStatus = [int]$_.Exception.Response.StatusCode } else { throw }
        }
        if ($duplicateStatus -ne 409) { throw "Expected idempotency conflict 409, got $duplicateStatus" }
        $deadline = [DateTime]::UtcNow.AddSeconds(30)
        do {
            $run = Invoke-RestMethod -NoProxy -Uri "http://127.0.0.1:28080/internal/v1/usage/queryRun/$id" -Headers $admin
            if ($run.settled) { break }
            Start-Sleep -Milliseconds 200
        } while ([DateTime]::UtcNow -lt $deadline)
        $ok = $run.settled -and $run.modelCallCount -eq 1 -and $run.actualTokens -eq 40 -and $run.calls[0].ledger_entries -eq 1 -and $run.calls[0].ledger_tokens -eq 40
        $rows.Add([pscustomobject]@{operationId=$id;scenario='C01+C02';attempts=2;expectedTokens=40;actualTokens=$run.actualTokens;ledgerEntries=$run.calls[0].ledger_entries;duplicateHttpStatus=$duplicateStatus;passed=$ok})
        if (-not $ok) { throw "Ledger validation failed for $id" }
    }
    $deadline = [DateTime]::UtcNow.AddSeconds(45)
    do {
        $totals = Invoke-RestMethod -NoProxy 'http://127.0.0.1:28080/internal/v1/usage/querySummary' -Headers $admin
        $redisValues = @(Invoke-BillingCompose exec -T redis redis-cli --raw HMGET agentops:quota:project-fund-agent consumed reserved active)
        $converged = $totals.ledgerTokens -eq $totals.projectedTokens -and [long]$redisValues[0] -eq $totals.ledgerTokens -and [long]$redisValues[1] -eq 0 -and [long]$redisValues[2] -eq 0
        if ($converged) { break }
        Start-Sleep -Milliseconds 300
    } while ([DateTime]::UtcNow -lt $deadline)
    if (-not $converged) { throw 'MySQL ledger / Kafka MySQL projection / Redis quota did not converge.' }
    $summary.result='PASS'; $summary.ledgerTokens=$totals.ledgerTokens; $summary.projectedTokens=$totals.projectedTokens; $summary.redisConsumed=[long]$redisValues[0]; $summary.reserved=0; $summary.active=0
} catch { $summary.error=$_.Exception.Message; throw } finally {
    $rows | Export-Csv (Join-Path $reportDir 'operations.csv') -NoTypeInformation -Encoding utf8
    $summary | ConvertTo-Json -Depth 6 | Set-Content (Join-Path $reportDir 'summary.json') -Encoding utf8
    Push-Location $RepoRoot
    try { @{runId=$runId;startedFromCommit=(& git rev-parse HEAD);workingTree=(& git status --porcelain);environment=$(if (Test-Path (Join-Path $StateDir 'deployment.json')) { Get-Content (Join-Path $StateDir 'deployment.json') -Raw | ConvertFrom-Json } else { 'Deployment incomplete' });sampleCount=$Count} | ConvertTo-Json -Depth 6 | Set-Content (Join-Path $reportDir 'manifest.json') -Encoding utf8 } finally { Pop-Location }
    Write-Host "Report: $reportDir"
}
$summary | ConvertTo-Json -Depth 6



