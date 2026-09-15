param(
    [int]$CompleteUsers = 10,
    [int]$CancelUsers = 10,
    [int]$RampSeconds = 0,
    [int]$SteadySeconds = 0,
    [int]$CancelAfterMs = 150,
    [int]$MaxP95Ms = 2000,
    [string]$GatewayUrl = 'http://localhost:18080',
    [string]$PrometheusUrl = 'http://localhost:19090',
    [int]$CleanupTimeoutSeconds = 30
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot

function Get-PrometheusValue([string]$Expression) {
    $encoded = [Uri]::EscapeDataString($Expression)
    $response = Invoke-RestMethod -Uri "$PrometheusUrl/api/v1/query?query=$encoded" -TimeoutSec 10
    if ($response.status -ne 'success') { throw "Prometheus query failed: $Expression" }
    if ($response.data.result.Count -eq 0) { return 0.0 }
    return [double]$response.data.result[0].value[1]
}

function Get-GatewayMetrics {
    return ((Invoke-WebRequest -Uri "$GatewayUrl/actuator/prometheus" -TimeoutSec 10).Content -split "`n")
}

function Get-MetricSamples([string[]]$Lines, [string]$MetricName, [hashtable]$RequiredLabels) {
    $samples = @{}
    $metricPattern = '^' + [Regex]::Escape($MetricName) + '(\{(?<labels>[^}]*)\})?\s+(?<value>[-+0-9.eE]+)'
    foreach ($line in $Lines) {
        if ($line -notmatch $metricPattern) { continue }
        $labels = $Matches.labels
        $sampleValue = [double]$Matches.value
        $matchesLabels = $true
        foreach ($entry in $RequiredLabels.GetEnumerator()) {
            $labelPattern = '(^|,)' + [Regex]::Escape([string]$entry.Key) + '="' + [Regex]::Escape([string]$entry.Value) + '"(,|$)'
            if ($labels -notmatch $labelPattern) { $matchesLabels = $false; break }
        }
        if (-not $matchesLabels) { continue }
        $key = if ($labels -match '(^|,)le="(?<le>[^"]+)"(,|$)') { $Matches.le } else { $labels }
        $samples[$key] = $sampleValue
    }
    return $samples
}

function Get-CounterDelta([string[]]$Before, [string[]]$After, [string]$MetricName, [hashtable]$Labels) {
    $beforeSamples = Get-MetricSamples $Before $MetricName $Labels
    $afterSamples = Get-MetricSamples $After $MetricName $Labels
    $beforeTotal = ($beforeSamples.Values | Measure-Object -Sum).Sum
    $afterTotal = ($afterSamples.Values | Measure-Object -Sum).Sum
    return [Math]::Max(0, [double]$afterTotal - [double]$beforeTotal)
}

function Get-HistogramQuantileUpperBound([string[]]$Before, [string[]]$After, [string]$MetricName, [hashtable]$Labels, [double]$Quantile) {
    $beforeBuckets = Get-MetricSamples $Before $MetricName $Labels
    $afterBuckets = Get-MetricSamples $After $MetricName $Labels
    $total = [double]$afterBuckets['+Inf'] - [double]$beforeBuckets['+Inf']
    if ($total -le 0) { return 0.0 }
    $target = $total * $Quantile
    $bounds = $afterBuckets.Keys | Where-Object { $_ -ne '+Inf' } | ForEach-Object { [double]$_ } | Sort-Object
    foreach ($bound in $bounds) {
        $key = [string]$bound
        if (-not $afterBuckets.ContainsKey($key)) {
            $key = $afterBuckets.Keys | Where-Object { $_ -ne '+Inf' -and [double]$_ -eq $bound } | Select-Object -First 1
        }
        $count = [double]$afterBuckets[$key] - [double]$beforeBuckets[$key]
        if ($count -ge $target) { return $bound }
    }
    return [double]::PositiveInfinity
}

function Wait-ForCleanup {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($CleanupTimeoutSeconds)
    do {
        $connections = Get-PrometheusValue 'agentops_gateway_active_connections'
        $finalizations = Get-PrometheusValue 'agentops_gateway_active_finalizations'
        if ($connections -eq 0 -and $finalizations -eq 0) {
            return @{ connections = $connections; finalizations = $finalizations }
        }
        Start-Sleep -Seconds 2
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "Gateway resources did not return to zero within $CleanupTimeoutSeconds seconds (connections=$connections, finalizations=$finalizations)."
}

Invoke-RestMethod -Uri "$GatewayUrl/actuator/health" -TimeoutSec 10 | Out-Null
Invoke-RestMethod -Uri "$PrometheusUrl/-/ready" -TimeoutSec 10 | Out-Null
$beforeMetrics = Get-GatewayMetrics
$heapBefore = (Get-MetricSamples $beforeMetrics 'jvm_memory_used_bytes' @{area='heap'}).Values | Measure-Object -Sum
$startedAt = [DateTimeOffset]::UtcNow

& "$PSScriptRoot/run-gatling.ps1" -CompleteUsers $CompleteUsers -CancelUsers $CancelUsers `
    -RampSeconds $RampSeconds -SteadySeconds $SteadySeconds -CancelAfterMs $CancelAfterMs -MaxP95Ms $MaxP95Ms -BaseUrl $GatewayUrl
$gatlingExitCode = $LASTEXITCODE

# Allow the final scrape to observe both the terminal counters and zero gauges.
Start-Sleep -Seconds 3
$cleanup = Wait-ForCleanup
$afterMetrics = Get-GatewayMetrics
$heapAfter = (Get-MetricSamples $afterMetrics 'jvm_memory_used_bytes' @{area='heap'}).Values | Measure-Object -Sum
$observedSeconds = [Math]::Max(10, [Math]::Ceiling(([DateTimeOffset]::UtcNow - $startedAt).TotalSeconds) + 5)
$heapMax = Get-PrometheusValue "max_over_time(sum(jvm_memory_used_bytes{area=`"heap`"})[${observedSeconds}s:2s])"
$hikariPendingMax = Get-PrometheusValue "max_over_time(hikaricp_connections_pending{pool=`"HikariPool-1`"}[${observedSeconds}s])"
$cancelP95 = Get-HistogramQuantileUpperBound $beforeMetrics $afterMetrics 'agentops_gateway_terminal_to_finalized_seconds_bucket' @{state='CANCELLED'} 0.95
$firstTokenP95 = Get-HistogramQuantileUpperBound $beforeMetrics $afterMetrics 'agentops_gateway_first_token_seconds_bucket' @{} 0.95
$cancelSuccess = Get-CounterDelta $beforeMetrics $afterMetrics 'agentops_gateway_finalizations_total' @{state='CANCELLED';outcome='success'}
$settledSuccess = Get-CounterDelta $beforeMetrics $afterMetrics 'agentops_gateway_finalizations_total' @{state='SETTLED';outcome='success'}
$finalizationFailures = (Get-CounterDelta $beforeMetrics $afterMetrics 'agentops_gateway_finalizations_total' @{outcome='failure'}) + (Get-CounterDelta $beforeMetrics $afterMetrics 'agentops_gateway_finalizations_total' @{outcome='rejected'})
$bufferOverflows = Get-CounterDelta $beforeMetrics $afterMetrics 'agentops_gateway_buffer_overflow_total' @{}

$summary = [ordered]@{
    generatedAt = [DateTimeOffset]::UtcNow.ToString('o')
    environment = 'local-mock-provider'
    passed = $false
    parameters = [ordered]@{
        completeUsers = $CompleteUsers
        cancelUsers = $CancelUsers
        rampSeconds = $RampSeconds
        steadySeconds = $SteadySeconds
        cancelAfterMs = $CancelAfterMs
        maxGatlingP95Ms = $MaxP95Ms
    }
    metrics = [ordered]@{
        firstTokenP95UpperBoundMs = [Math]::Round($firstTokenP95 * 1000, 2)
        cancelTerminalToFinalizedP95UpperBoundMs = [Math]::Round($cancelP95 * 1000, 2)
        settledFinalizations = [Math]::Round($settledSuccess, 0)
        cancelledFinalizations = [Math]::Round($cancelSuccess, 0)
        finalizationFailures = [Math]::Round($finalizationFailures, 0)
        bufferOverflows = [Math]::Round($bufferOverflows, 0)
        activeConnectionsAfterTest = $cleanup.connections
        activeFinalizationsAfterTest = $cleanup.finalizations
        heapUsedBeforeBytes = [double]$heapBefore.Sum
        heapUsedAfterBytes = [double]$heapAfter.Sum
        heapUsedMaxBytes = [Math]::Round($heapMax, 0)
        hikariPendingConnectionsMax = [Math]::Round($hikariPendingMax, 0)
    }
}

$failures = [System.Collections.Generic.List[string]]::new()
if ($gatlingExitCode -ne 0) { $failures.Add("Gatling failed with exit code $gatlingExitCode.") }
if ($finalizationFailures -gt 0) { $failures.Add("Observed $finalizationFailures failed or rejected finalizations.") }
if ($bufferOverflows -gt 0) { $failures.Add("Observed $bufferOverflows stream buffer overflows.") }
if ($cancelSuccess -lt $CancelUsers) { $failures.Add("Only $cancelSuccess of $CancelUsers cancellations finalized successfully.") }
if ($settledSuccess -lt $CompleteUsers) { $failures.Add("Only $settledSuccess of $CompleteUsers completed streams finalized successfully.") }
$summary.passed = $failures.Count -eq 0
$summary.failureReasons = @($failures)

$artifactDirectory = Join-Path $projectRoot 'artifacts/load-baselines'
New-Item -ItemType Directory -Force -Path $artifactDirectory | Out-Null
$artifactPath = Join-Path $artifactDirectory ("sse-baseline-{0}.json" -f [DateTimeOffset]::UtcNow.ToString('yyyyMMdd-HHmmss'))
$summary | ConvertTo-Json -Depth 5 | Set-Content -Path $artifactPath -Encoding utf8
$summary | ConvertTo-Json -Depth 5
Write-Host "Baseline artifact: $artifactPath"
if ($failures.Count -gt 0) { throw ($failures -join ' ') }
