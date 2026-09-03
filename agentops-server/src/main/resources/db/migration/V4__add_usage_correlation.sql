-- Groups every provider call made by one upstream Agent run without weakening request idempotency.
ALTER TABLE usage_reservation
  ADD COLUMN correlation_id VARCHAR(64) NULL AFTER request_id,
  ADD INDEX idx_usage_reservation_correlation (project_id, correlation_id, created_at);

UPDATE usage_reservation
SET correlation_id = request_id
WHERE correlation_id IS NULL;

ALTER TABLE usage_reservation
  MODIFY correlation_id VARCHAR(64) NOT NULL;
