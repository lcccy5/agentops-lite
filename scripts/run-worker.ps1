$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    # Use the caller's Java and the repository-local Maven settings so this works on any machine.
    mvn -pl agentops-worker -am spring-boot:run
} finally {
    Pop-Location
}
