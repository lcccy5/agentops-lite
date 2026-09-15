-- Fenced leases prevent a timed-out quota worker from overwriting a task reclaimed by another worker.
ALTER TABLE usage_quota_task
  ADD COLUMN lease_owner VARCHAR(128) NULL,
  ADD COLUMN lease_until TIMESTAMP(6) NULL,
  ADD COLUMN lease_version BIGINT NOT NULL DEFAULT 0,
  ADD INDEX idx_usage_quota_lease(status, lease_until);

-- A deployment may be upgrading while a previous process left an unfenced task in PROCESSING.
UPDATE usage_quota_task
SET status='PENDING', next_attempt_at=CURRENT_TIMESTAMP(6), lease_owner=NULL, lease_until=NULL
WHERE status='PROCESSING';
