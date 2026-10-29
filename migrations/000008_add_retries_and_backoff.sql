-- Add retry and error classification tracking to processing_results (DEC-04, INV-05, INV-06)
ALTER TABLE processing_results
ADD COLUMN IF NOT EXISTS attempt_count INT NOT NULL DEFAULT 1,
ADD COLUMN IF NOT EXISTS max_attempts INT NOT NULL DEFAULT 5,
ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_error TEXT,
ADD COLUMN IF NOT EXISTS error_type VARCHAR(50);

CREATE INDEX IF NOT EXISTS idx_processing_results_retry ON processing_results (status, next_retry_at) 
WHERE status = 'PENDING';
