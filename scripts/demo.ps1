param(
    [string]$FundAgentHome = $env:FUND_AGENT_HOME,
    [switch]$SkipBuild,
    [switch]$KeepRunning
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$stateDirectory = Join-Path $projectRoot '.demo'
$artifactDirectory = Join-Path $projectRoot 'artifacts'
$previousFundAgentHome = $env:FUND_AGENT_HOME
New-Item -ItemType Directory -Force -Path $stateDirectory, $artifactDirectory | Out-Null

# Waits for an HTTP health endpoint and surfaces the owning process log when startup fails.
function Wait-ServiceHealth([string]$Name, [string]$Url, [string]$ErrorLog) {
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        try {
            $health = Invoke-RestMethod -Uri $Url -TimeoutSec 2
            if ($health.status -eq 'UP') { Write-Host "$Name health            PASS"; return }
        } catch { }
        Start-Sleep -Seconds 1
    }
    if (Test-Path -LiteralPath $ErrorLog) { Get-Content -LiteralPath $ErrorLog -Tail 40 | Write-Host }
    throw "$Name did not become healthy at $Url."
}

# Starts a repository script in a hidden PowerShell process and records its exact PID for safe cleanup.
function Start-DemoScript([string]$Name, [string]$ScriptPath, [string[]]$Arguments = @()) {
    $stdout = Join-Path $stateDirectory "$Name.log"
    $stderr = Join-Path $stateDirectory "$Name-error.log"
    $argumentList = @('-NoProfile', '-ExecutionPolicy', 'Bypass', '-File', $ScriptPath) + $Arguments
    $process = Start-Process powershell.exe -WindowStyle Hidden -PassThru -ArgumentList $argumentList -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    Set-Content -LiteralPath (Join-Path $stateDirectory "$Name.pid") -Value $process.Id
    return $process
}

$report = [ordered]@{}
try {
    Push-Location $projectRoot
    & (Join-Path $PSScriptRoot 'check-environment.ps1')
    $report.environment = 'PASS'

    if (-not $SkipBuild) {
        mvn -DskipTests package
        if ($LASTEXITCODE -ne 0) { throw 'Maven build failed.' }
    }
    $report.build = if ($SkipBuild) { 'SKIPPED' } else { 'PASS' }

    & (Join-Path $PSScriptRoot 'start-local.ps1')
    $report.infrastructure = 'PASS'

    $server = Start-DemoScript 'server' (Join-Path $PSScriptRoot 'run-server.ps1')
    $worker = Start-DemoScript 'worker' (Join-Path $PSScriptRoot 'run-worker.ps1')
    Wait-ServiceHealth 'Server' 'http://localhost:18080/actuator/health' (Join-Path $stateDirectory 'server-error.log')
    Wait-ServiceHealth 'Worker' 'http://localhost:18082/actuator/health' (Join-Path $stateDirectory 'worker-error.log')
    $report.server = 'PASS'
    $report.worker = 'PASS'

    $onlineResult = & (Join-Path $PSScriptRoot 'demo-online-ledger.ps1') | ConvertFrom-Json
    $report.onlineLedger = $onlineResult.result
    $report.actualTokens = $onlineResult.actualTokens

    if (-not [string]::IsNullOrWhiteSpace($FundAgentHome)) {
        # Environment inheritance preserves paths containing spaces without fragile command-line quoting.
        $env:FUND_AGENT_HOME = $FundAgentHome
        $fundAgent = Start-DemoScript 'fund-agent' (Join-Path $PSScriptRoot 'run-fund-agent-through-gateway.ps1')
        Wait-ServiceHealth 'FundPilot' 'http://localhost:18081/actuator/health' (Join-Path $stateDirectory 'fund-agent-error.log')
        $releaseResult = & (Join-Path $PSScriptRoot 'run-evaluation-release-demo.ps1') | ConvertFrom-Json
        $report.evaluationRelease = 'PASS'
        $report.stablePassed = $releaseResult.stablePassed
        $report.candidatePassed = $releaseResult.candidatePassed
        $report.rollbackMillis = $releaseResult.rollbackMillis
    } else {
        $report.evaluationRelease = 'SKIPPED: pass -FundAgentHome or set FUND_AGENT_HOME'
    }

    $report.console = 'http://localhost:18080/console'
    $reportPath = Join-Path $artifactDirectory 'demo-report.json'
    $report | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $reportPath -Encoding utf8
    $report | ConvertTo-Json -Depth 5
    Write-Host "Demo report: $reportPath"
} finally {
    Pop-Location
    if ($null -eq $previousFundAgentHome) {
        Remove-Item Env:FUND_AGENT_HOME -ErrorAction SilentlyContinue
    } else {
        $env:FUND_AGENT_HOME = $previousFundAgentHome
    }
    if (-not $KeepRunning) { & (Join-Path $PSScriptRoot 'stop-demo.ps1') }
}
