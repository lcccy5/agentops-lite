$ErrorActionPreference = 'Stop'
$env:JAVA_HOME = 'C:\Users\25601\.jdks\ms-25.0.3'
mvn -gs D:\jijing-agent\.mvn\settings-global-public.xml test
mvn -gs D:\jijing-agent\.mvn\settings-global-public.xml -f D:\jijing-agent\pom.xml -pl fund-agent-runtime,fund-interface,fund-bootstrap -am -DskipTests=false test
