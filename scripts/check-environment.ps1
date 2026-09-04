param(
    [switch]$SkipPorts
)

$ErrorActionPreference = 'Stop'

# Validates tools before a build or demo mutates local state and fails with an actionable message.
function Assert-CommandAvailable([string]$Name, [string]$InstallHint) {
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "$Name is required. $InstallHint"
    }
    Write-Host ("{0,-24} PASS" -f $Name)
}

# Prevents the demo from silently attaching to an unrelated local service.
function Assert-PortAvailable([int]$Port) {
    $listener = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue
    if ($listener) { throw "Port $Port is already in use by process $($listener[0].OwningProcess)." }
    Write-Host ("Port {0,-19} PASS" -f $Port)
}

Assert-CommandAvailable 'mvn' 'Install Maven 3.9 or newer and add it to PATH.'
Assert-CommandAvailable 'docker' 'Install Docker Desktop and start Docker Engine.'

$mavenVersionLines = @(& mvn -version 2>&1)
$mavenVersionText = ($mavenVersionLines | Where-Object { $_ -match '^Apache Maven ' } | Select-Object -First 1).ToString()
$mavenJavaText = ($mavenVersionLines | Where-Object { $_ -match '^Java version:' } | Select-Object -First 1).ToString()
if ($mavenVersionText -notmatch 'Apache Maven (?<version>\d+\.\d+)') { throw 'Unable to parse the Maven version.' }
Write-Host ("Maven {0,-18} PASS" -f $Matches.version)
# Maven's runtime JDK is authoritative because every build and service launcher goes through Maven.
if ($mavenJavaText -notmatch 'Java version: (?<major>\d+)') { throw 'Unable to parse the JDK used by Maven.' }
if ([int]$Matches.major -lt 21) { throw "Java 21 or newer is required; Maven reports $mavenJavaText" }
Write-Host ("Maven Java {0,-13} PASS" -f $Matches.major)

docker info *> $null
if ($LASTEXITCODE -ne 0) { throw 'Docker CLI is installed, but Docker Engine is not running.' }
docker compose version *> $null
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose v2 is required.' }
Write-Host ("{0,-24} PASS" -f 'Docker Engine')
Write-Host ("{0,-24} PASS" -f 'Docker Compose')

if (-not $SkipPorts) {
    13306, 16379, 18080, 18082, 18090, 18474, 19092 | ForEach-Object { Assert-PortAvailable $_ }
}
