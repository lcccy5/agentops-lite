$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    # Install reactor dependencies before invoking Gatling in the isolated support module.
    mvn -pl agentops-test-support -am install -DskipTests
    mvn -pl agentops-test-support '-Dgatling.simulationClass=io.agentops.lite.load.SseGatewaySimulation' gatling:test
} finally {
    Pop-Location
}
