$ErrorActionPreference = 'Stop'
$env:JAVA_HOME = 'C:\Users\25601\.jdks\ms-25.0.3'
mvn -gs D:\jijing-agent\.mvn\settings-global-public.xml -pl agentops-server -am spring-boot:run
