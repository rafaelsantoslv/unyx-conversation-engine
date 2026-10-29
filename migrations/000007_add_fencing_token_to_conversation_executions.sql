-- Add fencing_token and lease_expires_at to conversation_executions for zombie worker protection (INV-04)
ALTER TABLE conversation_executions
ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 1;

-- Add fencing_token to processing_results as well so message result persistences can be guarded by fencing
ALTER TABLE processing_results
ADD COLUMN IF NOT EXISTS fencing_token BIGINT;
