#requires -Version 7.0
$ErrorActionPreference = 'Stop'
$RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
$StateDir = Join-Path $RepoRoot 'artifacts/billing-consistency/environment'
$ComposeFile = Join-Path $PSScriptRoot 'compose.yml'
function Invoke-BillingCompose {
    & docker compose -p agentops-billing-test -f $ComposeFile @args
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed: $($args -join ' ')" }
}
function Invoke-BillingSql([string]$Sql) {
    $result = @($Sql | & docker compose -p agentops-billing-test -f $ComposeFile exec -T -e MYSQL_PWD=billing-test-only mysql mysql -uagentops -Dagentops --batch --skip-column-names)
    if ($LASTEXITCODE -ne 0) { throw 'Test database query failed.' }
    return ($result -join "`n")
}
function Wait-BillingHealth([string]$Url) {
    $deadline = [DateTime]::UtcNow.AddSeconds(120)
    do {
        try { Write-Host "Checking $Url"; $health = Invoke-RestMethod -NoProxy -Uri $Url -TimeoutSec 5; if ($health.status -eq 'UP') { return } } catch { Write-Host $_.Exception.Message }
        Start-Sleep -Milliseconds 750
    } while ([DateTime]::UtcNow -lt $deadline)
    throw "Service not healthy: $Url. Inspect $StateDir logs."
}





