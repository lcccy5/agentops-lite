$ErrorActionPreference = 'Stop'

# Docker health checks make downstream demos wait for real readiness instead of an arbitrary sleep.
docker compose up -d --wait
docker compose ps
Write-Host 'Infrastructure started. Run server and worker in separate terminals with scripts/run-server.ps1 and scripts/run-worker.ps1.'
