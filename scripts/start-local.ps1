$ErrorActionPreference = 'Stop'
docker compose up -d
docker compose ps
Write-Host 'Infrastructure started. Run server and worker in separate terminals with scripts/run-server.ps1 and scripts/run-worker.ps1.'
