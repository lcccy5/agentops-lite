$ErrorActionPreference = 'Stop'
$env:JAVA_HOME = 'C:\Users\25601\.jdks\ms-25.0.3'
mvn -gs D:\jijing-agent\.mvn\settings-global-public.xml -pl agentops-test-support -am install -DskipTests
mvn -gs D:\jijing-agent\.mvn\settings-global-public.xml -pl agentops-test-support '-Dgatling.simulationClass=io.agentops.lite.load.SseGatewaySimulation' gatling:test
