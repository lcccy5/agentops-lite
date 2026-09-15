param(
    [int]$CompleteUsers = 10,
    [int]$CancelUsers = 10,
    [int]$RampSeconds = 0,
    [int]$SteadySeconds = 0,
    [int]$CancelAfterMs = 150,
    [int]$MaxP95Ms = 2000,
    [string]$BaseUrl = 'http://localhost:18080'
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $projectRoot
try {
    # Install reactor dependencies before invoking Gatling in the isolated support module.
    mvn -pl agentops-test-support -am install -DskipTests
    mvn -pl agentops-test-support `
        '-Dgatling.simulationClass=io.agentops.lite.load.SseGatewaySimulation' `
        "-DbaseUrl=$BaseUrl" `
        "-DcompleteUsers=$CompleteUsers" `
        "-DcancelUsers=$CancelUsers" `
        "-DrampSeconds=$RampSeconds" `
        "-DsteadySeconds=$SteadySeconds" `
        "-DcancelAfterMs=$CancelAfterMs" `
        "-DmaxP95Ms=$MaxP95Ms" `
        gatling:test
} finally {
    Pop-Location
}
