$ErrorActionPreference = 'Stop'

$baseUrl = 'http://localhost:18080'
$headers = @{'X-AgentOps-Admin-Token'='local-admin-token'}
$promptKey = 'fund-agent-system'
$suffix = Get-Date -Format 'yyyyMMddHHmmss'
$stableVersion = "fund-agent-stable-$suffix"
$candidateVersion = "fund-agent-candidate-$suffix"

function Invoke-AgentOpsPost([string]$path, [object]$body) {
    $json = $body | ConvertTo-Json -Depth 12
    return Invoke-RestMethod -Method Post -Uri "$baseUrl$path" -Headers $headers -ContentType 'application/json; charset=utf-8' -Body $json
}

# The marker controls deterministic WireMock behavior by prompt content, never by the version name.
$stable = Invoke-AgentOpsPost "/internal/v1/prompts/createVersion/$promptKey" @{
    version=$stableVersion
    template='你是基金分析助手。直接回答用户问题。'
}
$candidate = Invoke-AgentOpsPost "/internal/v1/prompts/createVersion/$promptKey" @{
    version=$candidateVersion
    template='AGENTOPS_EVAL_STRICT：必须选择正确工具和参数，引用可核验证据，禁止保证收益，并明确风险边界。'
}

$datasetPath = Join-Path $PSScriptRoot '..\agentops-test-support\src\main\resources\evals\fund-agent-prompt-e2e-v1.json'
$datasetResponse = & curl.exe --fail-with-body -sS -X POST "$baseUrl/internal/v1/evaluations/importDataset" `
    -H 'X-AgentOps-Admin-Token: local-admin-token' -H 'Content-Type: application/json; charset=utf-8' `
    --data-binary "@$datasetPath"
if ($LASTEXITCODE -ne 0) { throw "Dataset import failed with exit code $LASTEXITCODE" }
$dataset = $datasetResponse | ConvertFrom-Json

$job = Invoke-AgentOpsPost '/internal/v1/evaluations/createJob' @{
    datasetId=$dataset.datasetId
    promptKey=$promptKey
    stableVersion=$stableVersion
    candidateVersion=$candidateVersion
    # Tool calls and evidence increase deterministic candidate usage from ~40 to ~146 tokens.
    maxAverageTokenGrowthPercent=300
}

$jobView = $null
for ($attempt = 0; $attempt -lt 120; $attempt++) {
    $jobView = Invoke-RestMethod -Uri "$baseUrl/internal/v1/evaluations/queryJob/$($job.jobId)" -Headers $headers
    if ($jobView.status -in @('PASSED','FAILED')) { break }
    Start-Sleep -Seconds 2
}
if ($null -eq $jobView -or $jobView.status -notin @('PASSED','FAILED')) { throw 'Evaluation job did not finish within four minutes' }
if ($jobView.status -ne 'PASSED') { throw "Evaluation gate failed: $($jobView.gate.reasons_json)" }

$results = Invoke-RestMethod -Uri "$baseUrl/internal/v1/evaluations/queryResults/$($job.jobId)" -Headers $headers
$stablePassed = @($results | Where-Object { $_.prompt_version -eq $stableVersion -and $_.passed }).Count
$candidatePassed = @($results | Where-Object { $_.prompt_version -eq $candidateVersion -and $_.passed }).Count
$release = Invoke-AgentOpsPost '/internal/v1/releases/createRelease' @{
    promptKey=$promptKey
    environment='local'
    stableVersion=$stableVersion
    candidateVersion=$candidateVersion
    canaryPercent=5
    gateResultId=$jobView.gate.gate_result_id
}

$candidateSubject = $null
for ($index = 0; $index -lt 500; $index++) {
    $subject = "demo-subject-$index"
    $resolved = Invoke-AgentOpsPost "/internal/v1/prompts/resolvePrompt/$promptKey" @{environment='local';subjectKey=$subject;forcedVersion=$null}
    if ($resolved.variant -eq 'candidate') { $candidateSubject = $subject; break }
}
if ($null -eq $candidateSubject) { throw 'No 5% candidate subject found in deterministic search range' }
$stickyVersions = 1..5 | ForEach-Object {
    (Invoke-AgentOpsPost "/internal/v1/prompts/resolvePrompt/$promptKey" @{environment='local';subjectKey=$candidateSubject;forcedVersion=$null}).promptVersion
}
if (@($stickyVersions | Select-Object -Unique).Count -ne 1 -or $stickyVersions[0] -ne $candidateVersion) { throw 'Canary routing was not sticky' }

$rollbackTimer = [System.Diagnostics.Stopwatch]::StartNew()
$rolledBack = Invoke-AgentOpsPost "/internal/v1/releases/rollbackRelease/$($release.release_id)" @{}
$afterRollback = Invoke-AgentOpsPost "/internal/v1/prompts/resolvePrompt/$promptKey" @{environment='local';subjectKey=$candidateSubject;forcedVersion=$null}
$rollbackTimer.Stop()
if ($afterRollback.promptVersion -ne $stableVersion) { throw 'Rollback did not restore the stable prompt' }
if ($rollbackTimer.ElapsedMilliseconds -gt 5000) { throw 'Rollback exceeded the five-second acceptance limit' }

[PSCustomObject]@{
    datasetId=$dataset.datasetId
    jobId=$job.jobId
    gateResultId=$jobView.gate.gate_result_id
    stablePassed="$stablePassed/12"
    candidatePassed="$candidatePassed/12"
    releaseId=$release.release_id
    candidateSubject=$candidateSubject
    stickyCandidateChecks=$stickyVersions.Count
    rollbackVersion=$afterRollback.promptVersion
    rollbackMillis=$rollbackTimer.ElapsedMilliseconds
} | ConvertTo-Json -Depth 5
