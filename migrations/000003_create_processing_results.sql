-- Create processing_results table for Execution Lifecycle and Idempotency tracking (DEC-05)
CREATE TABLE IF NOT EXISTS processing_results (
    id UUID PRIMARY KEY,
    tenant_id VARCHAR(255) NOT NULL,
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    message_id UUID NOT NULL,
    status VARCHAR(50) NOT NULL, -- RUNNING, SUCCEEDED, FAILED
    output_payload JSONB,
    error_message TEXT,
    execution_time_ms BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT uq_processing_results_msg UNIQUE (tenant_id, conversation_id, message_id)
);

CREATE INDEX IF NOT EXISTS idx_processing_results_lookup ON processing_results (tenant_id, conversation_id, message_id);
CREATE INDEX IF NOT EXISTS idx_processing_results_status ON processing_results (status);
