param(
    [switch]$KeepInfrastructure
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$stateDirectory = Join-Path $projectRoot '.demo'

# Stops only the process recorded by this repository so other Java processes remain untouched.
function Stop-DemoProcess([string]$Name) {
    $pidFile = Join-Path $stateDirectory "$Name.pid"
    if (-not (Test-Path -LiteralPath $pidFile)) { return }
    $processId = [int](Get-Content -Raw -LiteralPath $pidFile)
    $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
    if ($process) {
        Stop-Process -Id $processId -ErrorAction Stop
        Write-Host "Stopped $Name process $processId."
    }
    Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
}

Stop-DemoProcess 'fund-agent'
Stop-DemoProcess 'worker'
Stop-DemoProcess 'server'

if (-not $KeepInfrastructure) {
    Push-Location $projectRoot
    try { docker compose down } finally { Pop-Location }
}
