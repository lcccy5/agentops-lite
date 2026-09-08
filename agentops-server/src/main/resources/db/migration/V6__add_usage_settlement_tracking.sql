-- Separates execution completion from the eventual accounting decision.
ALTER TABLE provider_config
  ADD COLUMN settlement_mode VARCHAR(32) NOT NULL DEFAULT 'ESTIMATE_FALLBACK',
  ADD COLUMN usage_query_path VARCHAR(500) NULL,
  ADD COLUMN supports_stream_cancellation BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE usage_reservation
  ADD COLUMN settlement_status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
  ADD COLUMN execution_outcome VARCHAR(32) NULL,
  ADD COLUMN input_tokens BIGINT NULL,
  ADD COLUMN output_tokens BIGINT NULL,
  ADD COLUMN estimator_version VARCHAR(64) NULL,
  ADD COLUMN settlement_deadline TIMESTAMP(6) NULL,
  ADD COLUMN quota_sync_status VARCHAR(32) NOT NULL DEFAULT 'PENDING';

-- An upstream attempt is distinct from the client request so a retry can retain its own audit trail.
CREATE TABLE usage_provider_attempt (
  attempt_id VARCHAR(64) PRIMARY KEY,
  reservation_id VARCHAR(64) NOT NULL,
  attempt_no INT NOT NULL,
  provider_endpoint_id VARCHAR(64) NOT NULL,
  requested_model VARCHAR(128) NULL,
  actual_model VARCHAR(128) NULL,
  provider_trace_id VARCHAR(256) NULL,
  provider_generation_id VARCHAR(256) NULL,
  started_at TIMESTAMP(6) NOT NULL,
  updated_at TIMESTAMP(6) NOT NULL,
  UNIQUE KEY uk_usage_attempt(reservation_id,attempt_no),
  INDEX idx_usage_attempt_generation(provider_generation_id)
);

-- Database leases let workers retry asynchronous usage lookups without duplicate settlement.
CREATE TABLE usage_lookup_task (
  task_id VARCHAR(64) PRIMARY KEY,
  reservation_id VARCHAR(64) NOT NULL UNIQUE,
  attempt_id VARCHAR(64) NOT NULL,
  provider_generation_id VARCHAR(256) NOT NULL,
  usage_query_path VARCHAR(500) NOT NULL,
  provider_base_url VARCHAR(500) NOT NULL,
  status VARCHAR(32) NOT NULL,
  attempts INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMP(6) NOT NULL,
  deadline_at TIMESTAMP(6) NOT NULL,
  lease_owner VARCHAR(128) NULL,
  lease_until TIMESTAMP(6) NULL,
  last_error_code VARCHAR(128) NULL,
  created_at TIMESTAMP(6) NOT NULL,
  updated_at TIMESTAMP(6) NOT NULL,
  INDEX idx_usage_lookup_due(status,next_attempt_at)
);

-- Redis remains an online projection; every cross-system change has a durable retry record.
CREATE TABLE usage_quota_task (
  task_id VARCHAR(64) PRIMARY KEY,
  reservation_id VARCHAR(64) NOT NULL,
  operation_id VARCHAR(64) NOT NULL UNIQUE,
  action_type VARCHAR(32) NOT NULL,
  token_value BIGINT NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'PENDING',
  attempts INT NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMP(6) NOT NULL,
  last_error_code VARCHAR(128) NULL,
  created_at TIMESTAMP(6) NOT NULL,
  updated_at TIMESTAMP(6) NOT NULL,
  INDEX idx_usage_quota_due(status,next_attempt_at)
);

-- Existing multi-purpose rows retain their ledger truth while moving to explicit settlement state.
UPDATE usage_reservation r
LEFT JOIN usage_ledger l ON l.reservation_id=r.reservation_id
SET r.settlement_status=CASE WHEN l.ledger_id IS NULL THEN 'PENDING' ELSE 'FINAL' END,
    r.execution_outcome=CASE WHEN r.status='RECONCILIATION_PENDING' THEN 'UNKNOWN' ELSE r.status END,
    r.quota_sync_status=CASE WHEN r.failure_code='REDIS_FINALIZE_FAILED' THEN 'FAILED' ELSE 'APPLIED' END
WHERE r.status='RECONCILIATION_PENDING';
