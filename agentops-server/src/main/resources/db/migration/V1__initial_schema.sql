CREATE TABLE agent_project (
  project_id VARCHAR(64) PRIMARY KEY, name VARCHAR(128) NOT NULL,
  token_limit BIGINT NOT NULL, max_concurrency INT NOT NULL,
  default_max_tokens INT NOT NULL, project_max_tokens INT NOT NULL,
  created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
);
CREATE TABLE project_api_key (
  api_key_id VARCHAR(64) PRIMARY KEY, project_id VARCHAR(64) NOT NULL,
  key_hash CHAR(64) NOT NULL UNIQUE, enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  CONSTRAINT fk_api_project FOREIGN KEY(project_id) REFERENCES agent_project(project_id)
);
CREATE TABLE provider_config (
  provider_id VARCHAR(64) PRIMARY KEY, project_id VARCHAR(64) NOT NULL,
  base_url VARCHAR(500) NOT NULL, model_name VARCHAR(128), enabled BOOLEAN NOT NULL DEFAULT TRUE,
  CONSTRAINT fk_provider_project FOREIGN KEY(project_id) REFERENCES agent_project(project_id)
);
CREATE TABLE usage_reservation (
  reservation_id VARCHAR(64) PRIMARY KEY, request_id VARCHAR(64) NOT NULL UNIQUE,
  project_id VARCHAR(64) NOT NULL, idempotency_key VARCHAR(128) NOT NULL,
  reserved_tokens BIGINT NOT NULL, actual_tokens BIGINT NULL,
  status VARCHAR(40) NOT NULL, usage_source VARCHAR(32) NULL, prompt_version VARCHAR(128) NULL,
  provider_started BOOLEAN NOT NULL DEFAULT FALSE, failure_code VARCHAR(80) NULL,
  expires_at TIMESTAMP(6) NOT NULL, created_at TIMESTAMP(6) NOT NULL,
  updated_at TIMESTAMP(6) NOT NULL,
  UNIQUE KEY uk_reservation_idempotency(project_id,idempotency_key),
  INDEX idx_reservation_status_expiry(status,expires_at)
);
CREATE TABLE usage_ledger (
  ledger_id VARCHAR(64) PRIMARY KEY, reservation_id VARCHAR(64) NOT NULL,
  project_id VARCHAR(64) NOT NULL, ledger_type VARCHAR(40) NOT NULL,
  related_ledger_id VARCHAR(64) NULL, token_delta BIGINT NOT NULL,
  cost_delta DECIMAL(20,8) NOT NULL DEFAULT 0, prompt_version VARCHAR(128) NULL,
  occurred_at TIMESTAMP(6) NOT NULL,
  UNIQUE KEY uk_ledger_reservation_type(reservation_id,ledger_type,related_ledger_id)
);
CREATE TABLE usage_outbox (
  event_id VARCHAR(64) PRIMARY KEY, ledger_id VARCHAR(64) NOT NULL UNIQUE,
  event_key VARCHAR(64) NOT NULL, payload_json JSON NOT NULL,
  status VARCHAR(24) NOT NULL DEFAULT 'PENDING', attempts INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMP(6) NOT NULL, published_at TIMESTAMP(6) NULL,
  created_at TIMESTAMP(6) NOT NULL, INDEX idx_usage_outbox(status,next_attempt_at)
);
CREATE TABLE usage_projection (
  project_id VARCHAR(64) PRIMARY KEY, total_tokens BIGINT NOT NULL DEFAULT 0,
  total_cost DECIMAL(20,8) NOT NULL DEFAULT 0, updated_at TIMESTAMP(6) NOT NULL
);
CREATE TABLE usage_projection_applied (
  ledger_id VARCHAR(64) PRIMARY KEY, applied_at TIMESTAMP(6) NOT NULL
);
CREATE TABLE usage_reconciliation (
  reconciliation_id VARCHAR(64) PRIMARY KEY, project_id VARCHAR(64) NOT NULL,
  discrepancy_type VARCHAR(64) NOT NULL, expected_value BIGINT NOT NULL,
  actual_value BIGINT NOT NULL, suggested_action VARCHAR(500) NOT NULL,
  detected_at TIMESTAMP(6) NOT NULL
);
CREATE TABLE prompt_version (
  prompt_version_id VARCHAR(64) PRIMARY KEY, project_id VARCHAR(64) NOT NULL,
  prompt_key VARCHAR(128) NOT NULL, version VARCHAR(128) NOT NULL,
  template_text MEDIUMTEXT NOT NULL, template_hash CHAR(64) NOT NULL,
  created_at TIMESTAMP(6) NOT NULL, UNIQUE KEY uk_prompt_version(project_id,prompt_key,version)
);
CREATE TABLE eval_dataset (
  dataset_id VARCHAR(64) PRIMARY KEY, project_id VARCHAR(64) NOT NULL,
  name VARCHAR(128) NOT NULL, created_at TIMESTAMP(6) NOT NULL
);
CREATE TABLE eval_case (
  case_id VARCHAR(128) NOT NULL, dataset_id VARCHAR(64) NOT NULL,
  definition_json JSON NOT NULL, PRIMARY KEY(dataset_id,case_id)
);
CREATE TABLE eval_job (
  job_id VARCHAR(64) PRIMARY KEY, project_id VARCHAR(64) NOT NULL,
  dataset_id VARCHAR(64) NOT NULL, prompt_key VARCHAR(128) NOT NULL,
  stable_version VARCHAR(128) NOT NULL, candidate_version VARCHAR(128) NOT NULL,
  max_token_growth_percent BIGINT NOT NULL, status VARCHAR(32) NOT NULL,
  created_at TIMESTAMP(6) NOT NULL, completed_at TIMESTAMP(6) NULL
);
CREATE TABLE eval_job_case (
  job_id VARCHAR(64) NOT NULL, case_id VARCHAR(128) NOT NULL,
  prompt_version VARCHAR(128) NOT NULL, status VARCHAR(32) NOT NULL,
  attempts INT NOT NULL DEFAULT 0, PRIMARY KEY(job_id,case_id,prompt_version)
);
CREATE TABLE eval_result (
  result_id VARCHAR(64) PRIMARY KEY, job_id VARCHAR(64) NOT NULL,
  case_id VARCHAR(128) NOT NULL, prompt_version VARCHAR(128) NOT NULL,
  passed BOOLEAN NOT NULL, hard_safety BOOLEAN NOT NULL, score_json JSON NOT NULL,
  observation_json JSON NOT NULL, input_tokens BIGINT NOT NULL, output_tokens BIGINT NOT NULL,
  first_token_millis BIGINT NOT NULL, total_millis BIGINT NOT NULL, created_at TIMESTAMP(6) NOT NULL,
  UNIQUE KEY uk_eval_result(job_id,case_id,prompt_version)
);
CREATE TABLE eval_dispatch_outbox (
  event_id VARCHAR(64) PRIMARY KEY, job_id VARCHAR(64) NOT NULL,
  case_id VARCHAR(128) NOT NULL, prompt_version VARCHAR(128) NOT NULL,
  event_key VARCHAR(128) NOT NULL, payload_json JSON NOT NULL,
  status VARCHAR(24) NOT NULL DEFAULT 'PENDING', attempts INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMP(6) NOT NULL, published_at TIMESTAMP(6) NULL,
  UNIQUE KEY uk_eval_dispatch(job_id,case_id,prompt_version), INDEX idx_eval_outbox(status,next_attempt_at)
);
CREATE TABLE eval_gate_result (
  gate_result_id VARCHAR(64) PRIMARY KEY, job_id VARCHAR(64) NOT NULL UNIQUE,
  passed BOOLEAN NOT NULL, reasons_json JSON NOT NULL, created_at TIMESTAMP(6) NOT NULL
);
CREATE TABLE prompt_release (
  release_id VARCHAR(64) PRIMARY KEY, project_id VARCHAR(64) NOT NULL,
  prompt_key VARCHAR(128) NOT NULL, environment_name VARCHAR(64) NOT NULL,
  stable_version VARCHAR(128) NOT NULL, candidate_version VARCHAR(128) NOT NULL,
  canary_percent INT NOT NULL, gate_result_id VARCHAR(64) NOT NULL,
  status VARCHAR(32) NOT NULL, created_at TIMESTAMP(6) NOT NULL, rolled_back_at TIMESTAMP(6) NULL,
  INDEX idx_release_resolve(project_id,prompt_key,environment_name,status,created_at)
);
