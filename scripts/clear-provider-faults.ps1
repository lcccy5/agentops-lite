$ErrorActionPreference = 'Stop'
Invoke-RestMethod -Headers @{'User-Agent'='curl/8.0'} -Method Delete -Uri 'http://localhost:18474/proxies/provider/toxics/latency'
