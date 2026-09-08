. "$PSScriptRoot/common.ps1"
foreach ($service in @('worker','server')) {
    $statePath = Join-Path $StateDir "$service.json"
    if (-not (Test-Path -LiteralPath $statePath)) { continue }
    $state = Get-Content -Raw -LiteralPath $statePath | ConvertFrom-Json
    $process = Get-Process -Id $state.pid -ErrorAction SilentlyContinue
    if ($process) {
        if ($process.StartTime.ToUniversalTime().Ticks -ne ([datetime]$state.startTime).ToUniversalTime().Ticks) { throw "PID reused for $service; refusing to stop it." }
        Stop-Process -Id $state.pid
        $process.WaitForExit(15000) | Out-Null
    }
    Remove-Item -LiteralPath $statePath
}
Invoke-BillingCompose down
Write-Host 'Test services stopped. MySQL/Redis volumes and reports retained.'



