-- Alter processing_results to support execution lease and robust idempotency control
ALTER TABLE processing_results 
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_processing_results_lease ON processing_results (status, lease_expires_at);
