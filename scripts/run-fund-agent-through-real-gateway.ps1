$ErrorActionPreference = 'Stop'

# Routes FundPilot's existing real model through AgentOps; the browser never sees this API key.
$env:AI_CHAT_BASE_URL = 'http://localhost:18080'
$env:AI_CHAT_API_KEY = 'agentops-dev-key'
$env:AI_CHAT_PROVIDER = 'openai'
$env:AI_CHAT_MODEL = 'qwen3.7-max'
$env:FUND_AGENT_ENABLED = 'true'
$env:AGENTOPS_PROMPT_ENABLED = 'true'
$env:AGENTOPS_BASE_URL = 'http://localhost:18080'
$env:AGENTOPS_ADMIN_TOKEN = 'local-admin-token'
$env:AGENTOPS_ENVIRONMENT = 'local'
$env:MYSQL_URL = 'jdbc:mysql://localhost:13306/jijing_agent?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai'
$env:MYSQL_USERNAME = 'root'
$env:MYSQL_PASSWORD = 'root'
$env:REDIS_HOST = 'localhost'
$env:REDIS_PORT = '16379'
$env:SERVER_PORT = '18081'
$env:FUND_JWT_SIGNING_KEY = 'agentops-local-fund-jwt-signing-key-32-bytes-minimum'
$env:FUND_SECURITY_REFRESH_COOKIE_SECURE = 'false'
$env:FUND_KNOWLEDGE_WORKER_ENABLED = 'false'

Write-Host 'FundPilot is configured to call the real model through AgentOps at http://localhost:18080.'
mvn -f D:jijing-agentpom.xml -pl fund-bootstrap -am spring-boot:run
