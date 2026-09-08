param([switch]$SkipBuild)
. "$PSScriptRoot/common.ps1"
New-Item -ItemType Directory -Force $StateDir | Out-Null
foreach ($port in @(28080,28082)) {
    if (Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue) { throw "Port $port is occupied. Stop the previous test environment first." }
}
& docker info --format '{{.ServerVersion}}'
if ($LASTEXITCODE -ne 0) { throw 'Start Docker Desktop first.' }
$mavenInfo = (& mvn -version | Out-String)
if ($mavenInfo -notmatch 'Java version: (\d+)') { throw 'Cannot determine Maven Java version.' }
if ([int]$Matches[1] -lt 21) { throw 'Maven requires Java 21 or newer.' }
if ($mavenInfo -notmatch 'runtime: ([^\r\n]+)') { throw 'Cannot locate Maven Java runtime.' }
$javaExe = Join-Path $Matches[1].Trim() 'bin/java.exe'
if (-not $SkipBuild) {
    Push-Location $RepoRoot
    try { & mvn -B -ntp package; if ($LASTEXITCODE -ne 0) { throw 'Build failed.' } } finally { Pop-Location }
}
Invoke-BillingCompose up -d --wait --wait-timeout 180
$envValues = @{
    MYSQL_URL='jdbc:mysql://localhost:23306/agentops?useUnicode=true&characterEncoding=utf8&serverTimezone=UTC'
    MYSQL_USER='agentops'; MYSQL_PASSWORD='billing-test-only'; REDIS_HOST='localhost'; REDIS_PORT='26379'
    KAFKA_BOOTSTRAP_SERVERS='localhost:29092'; SERVER_PORT='28080'; WORKER_MANAGEMENT_PORT='28082'
    SERVER_ADDRESS='127.0.0.1'; PROVIDER_BASE_URL='http://127.0.0.1:28090'; PROVIDER_API_KEY='wiremock-key'
    AGENTOPS_ADMIN_TOKEN='billing-test-admin'; SPRING_KAFKA_CONSUMER_GROUP_ID='billing-test-worker-v1'
    PROVIDER_SETTLEMENT_MODE='ESTIMATE_FALLBACK'; AGENTOPS_WORKER_RECOVERY_DELAY_MS='10000'
}
$previous = @{}
try {
    foreach ($key in $envValues.Keys) { $previous[$key] = [Environment]::GetEnvironmentVariable($key,'Process'); [Environment]::SetEnvironmentVariable($key,$envValues[$key],'Process') }
    foreach ($service in @('server','worker')) {
        # SERVER_PORT has higher precedence than the worker YAML placeholder.
        $env:SERVER_PORT = if ($service -eq 'server') { '28080' } else { '28082' }
        $jar = Get-ChildItem (Join-Path $RepoRoot "agentops-$service/target/*-exec.jar") | Select-Object -First 1
        if (-not $jar) { throw "Missing executable jar for $service" }
        $process = Start-Process -FilePath $javaExe -ArgumentList @('-Xms128m','-Xmx512m','-jar',('"' + $jar.FullName + '"')) -WorkingDirectory $RepoRoot -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $StateDir "$service.log") -RedirectStandardError (Join-Path $StateDir "$service-error.log")
        @{pid=$process.Id;startTime=$process.StartTime.ToUniversalTime().ToString('o');jar=$jar.FullName} | ConvertTo-Json | Set-Content (Join-Path $StateDir "$service.json") -Encoding utf8
        $port = if ($service -eq 'server') { 28080 } else { 28082 }
        Wait-BillingHealth "http://127.0.0.1:$port/actuator/health"
        if ($service -eq 'server') { Invoke-BillingSql "update provider_config set base_url='http://127.0.0.1:28090' where provider_id='provider-wiremock';" | Out-Null }
        Write-Host "$service UP on $port"
    }
    @{startedAt=[DateTime]::UtcNow.ToString('o');java=$javaExe;recoveryDelayMs=10000;provider='WireMock, fixed total_tokens=40';server='http://127.0.0.1:28080';worker='http://127.0.0.1:28082'} | ConvertTo-Json | Set-Content (Join-Path $StateDir 'deployment.json') -Encoding utf8
} finally {
    foreach ($key in $previous.Keys) { [Environment]::SetEnvironmentVariable($key,$previous[$key],'Process') }
}
Write-Host 'Ready. Run ./tests/billing-consistency/verify.ps1'



