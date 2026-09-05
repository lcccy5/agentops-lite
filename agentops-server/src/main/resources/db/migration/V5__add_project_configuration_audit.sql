CREATE TABLE project_configuration_audit (
  audit_id VARCHAR(64) PRIMARY KEY,
  project_id VARCHAR(64) NOT NULL,
  action VARCHAR(64) NOT NULL,
  before_json TEXT NOT NULL,
  after_json TEXT NOT NULL,
  created_at TIMESTAMP(6) NOT NULL,
  INDEX idx_project_configuration_audit(project_id, created_at)
);
