$ErrorActionPreference = 'Stop'
$headers = @{Authorization='Bearer agentops-dev-key'; 'Idempotency-Key'=[guid]::NewGuid().ToString(); 'Prompt-Version'='fund-agent-stable-v1'}
$body = @{model='deterministic-fund-model'; stream=$false; max_tokens=128; messages=@(@{role='user';content='分析基金风险'})} | ConvertTo-Json -Depth 8
$response = Invoke-RestMethod -Method Post -Uri 'http://localhost:18080/v1/chat/completions' -Headers $headers -ContentType 'application/json' -Body $body
$response | ConvertTo-Json -Depth 8
Start-Sleep -Seconds 2
Invoke-RestMethod -Uri 'http://localhost:18080/internal/v1/usage/querySummary' -Headers @{'X-AgentOps-Admin-Token'='local-admin-token'} | ConvertTo-Json
