INSERT INTO agent_project(project_id,name,token_limit,max_concurrency,default_max_tokens,project_max_tokens)
VALUES ('project-fund-agent','Fund Research Agent',1000000,64,1024,4096);
-- SHA-256(agentops-dev-key); only the digest is persisted and accepted by the gateway.
INSERT INTO project_api_key(api_key_id,project_id,key_hash) VALUES ('key-local','project-fund-agent','d7c7972767ccbdf457f58d5873cc1156322e54840f295166ad73131362d5b190');
INSERT INTO provider_config(provider_id,project_id,base_url,model_name) VALUES ('provider-wiremock','project-fund-agent','http://wiremock:8080','deterministic-fund-model');
