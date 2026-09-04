param(
    [string]$FundAgentHome = $env:FUND_AGENT_HOME
)

$ErrorActionPreference = 'Stop'
if ([string]::IsNullOrWhiteSpace($FundAgentHome)) {
    throw 'FundAgentHome is required. Pass -FundAgentHome or set FUND_AGENT_HOME to the FundPilot repository.'
}
$fundAgentPom = Join-Path $FundAgentHome 'pom.xml'
if (-not (Test-Path -LiteralPath $fundAgentPom)) { throw "FundPilot pom.xml was not found at $fundAgentPom" }
$env:AI_CHAT_BASE_URL = 'http://localhost:18080'
$env:AI_CHAT_API_KEY = 'agentops-dev-key'
$env:AI_CHAT_PROVIDER = 'openai'
$env:AI_CHAT_MODEL = 'deterministic-fund-model'
$env:FUND_AGENT_ENABLED = 'true'
$env:FUND_AGENT_PROMPT_VERSION = 'fund-agent-v1'
$env:FUND_KNOWLEDGE_ENABLED = 'true'
$env:AGENTOPS_PROMPT_ENABLED = 'true'
$env:AGENTOPS_BASE_URL = 'http://localhost:18080'
$env:MYSQL_URL = 'jdbc:mysql://localhost:13306/jijing_agent?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai'
$env:MYSQL_USERNAME = 'agentops'
$env:MYSQL_PASSWORD = 'agentops'
$env:REDIS_HOST = 'localhost'
$env:REDIS_PORT = '16379'
$env:SERVER_PORT = '18081'
$env:FUND_JWT_SIGNING_KEY = 'agentops-local-eval-signing-key-32-bytes-minimum'
$env:SPRING_PROFILES_ACTIVE = 'local,agent-eval'
# The external repository remains independently buildable; AgentOps passes only runtime integration settings.
mvn -f $fundAgentPom -pl fund-bootstrap -am spring-boot:run
