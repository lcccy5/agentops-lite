$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
& (Join-Path $PSScriptRoot 'check-environment.ps1') -SkipPorts
Push-Location $projectRoot
try {
    # The verify phase runs unit tests first and Docker-backed *IT integration tests through Failsafe.
    mvn clean verify
} finally {
    Pop-Location
}
