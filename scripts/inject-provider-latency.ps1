$ErrorActionPreference = 'Stop'
$headers = @{'User-Agent'='curl/8.0'}
$proxy = @{name='provider';listen='0.0.0.0:18091';upstream='wiremock:8080'} | ConvertTo-Json
try { Invoke-RestMethod -Headers $headers -Method Post -Uri 'http://localhost:18474/proxies' -ContentType 'application/json' -Body $proxy } catch {
    if ($_.Exception.Response.StatusCode -ne 409) { throw }
    Write-Host 'Proxy already exists.'
}
$toxic = @{name='latency';type='latency';stream='downstream';toxicity=1.0;attributes=@{latency=1200;jitter=100}} | ConvertTo-Json -Depth 4
Invoke-RestMethod -Headers $headers -Method Post -Uri 'http://localhost:18474/proxies/provider/toxics' -ContentType 'application/json' -Body $toxic | ConvertTo-Json
