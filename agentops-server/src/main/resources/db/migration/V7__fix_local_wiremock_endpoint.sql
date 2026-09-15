-- The documented development topology runs Server on the host and WireMock in Docker.
-- Only repair the deterministic seed endpoint; operator-managed provider URLs are untouched.
UPDATE provider_config
SET base_url='http://localhost:18090'
WHERE provider_id='provider-wiremock'
  AND project_id='project-fund-agent'
  AND base_url='http://wiremock:8080';
