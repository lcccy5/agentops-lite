$ErrorActionPreference = 'Stop'
$datasetPath = Join-Path $PSScriptRoot '..\agentops-test-support\src\main\resources\evals\fund-agent-prompt-e2e-v1.json'
# curl's binary file upload preserves the UTF-8 dataset on Windows PowerShell 5.1.
& curl.exe --fail-with-body -sS -X POST 'http://localhost:18080/internal/v1/evaluations/importDataset' `
    -H 'X-AgentOps-Admin-Token: local-admin-token' -H 'Content-Type: application/json; charset=utf-8' `
    --data-binary "@$datasetPath"
if ($LASTEXITCODE -ne 0) { throw "Dataset import failed with exit code $LASTEXITCODE" }
