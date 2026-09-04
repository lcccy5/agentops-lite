$ErrorActionPreference = 'Stop'
$baseUrl = 'http://localhost:18080'
$requestId = [guid]::NewGuid().ToString()
$headers = @{Authorization='Bearer agentops-dev-key'; 'Idempotency-Key'=$requestId; 'X-AgentOps-Request-Id'=$requestId; 'X-AgentOps-Correlation-Id'=$requestId; 'Prompt-Version'='fund-agent-stable-v1'}
$adminHeaders = @{'X-AgentOps-Admin-Token'='local-admin-token'}
$body = @{model='deterministic-fund-model'; stream=$false; max_tokens=128; messages=@(@{role='user';content='分析基金风险'})} | ConvertTo-Json -Depth 8
$response = Invoke-RestMethod -Method Post -Uri "$baseUrl/v1/chat/completions" -Headers $headers -ContentType 'application/json' -Body $body

# Poll persisted facts because settlement and Kafka projection deliberately finish off the response thread.
$run = $null
$summary = $null
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    $run = Invoke-RestMethod -Uri "$baseUrl/internal/v1/usage/queryRun/$requestId" -Headers $adminHeaders
    $summary = Invoke-RestMethod -Uri "$baseUrl/internal/v1/usage/querySummary" -Headers $adminHeaders
    if ($run.settled -and $run.actualTokens -gt 0 -and $summary.ledgerTokens -eq $summary.projectedTokens) { break }
    Start-Sleep -Milliseconds 500
}

if (-not $run.settled) { throw 'Reservation did not reach a terminal state.' }
if ($run.modelCallCount -ne 1) { throw "Expected one model call, found $($run.modelCallCount)." }
if ($run.actualTokens -le 0) { throw 'Provider usage was not persisted.' }
if ($run.calls[0].ledger_entries -ne 1) { throw 'Expected exactly one immutable ledger entry.' }
if ($summary.ledgerTokens -ne $summary.projectedTokens) { throw 'Kafka projection did not converge to the immutable ledger.' }

[PSCustomObject]@{
    requestId=$requestId
    providerResponse=$response.id
    reservationSettled=$run.settled
    actualTokens=$run.actualTokens
    ledgerEntries=$run.calls[0].ledger_entries
    ledgerTokens=$summary.ledgerTokens
    projectedTokens=$summary.projectedTokens
    result='PASS'
} | ConvertTo-Json -Depth 5
