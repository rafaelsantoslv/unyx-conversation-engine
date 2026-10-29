-- Add sequence_number to processing_results for strict conversational ordering checks (INV-02)
ALTER TABLE processing_results
ADD COLUMN IF NOT EXISTS sequence_number BIGINT NOT NULL DEFAULT 1;

CREATE INDEX IF NOT EXISTS idx_processing_results_conv_seq ON processing_results (conversation_id, sequence_number);
