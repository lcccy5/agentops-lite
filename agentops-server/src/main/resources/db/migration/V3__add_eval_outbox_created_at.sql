ALTER TABLE eval_dispatch_outbox
  ADD COLUMN created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  ADD INDEX idx_eval_outbox_created(status, next_attempt_at, created_at);
