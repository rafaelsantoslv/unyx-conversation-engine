-- Migration: 000010_add_reconciler_and_attempt_to_conversation_executions.sql
-- Adiciona attempt_count e max_attempts em conversation_executions para suportar o Reconciliador de Execuções Órfãs (INV-04, INV-05)

ALTER TABLE conversation_executions
ADD COLUMN IF NOT EXISTS attempt_count INT NOT NULL DEFAULT 1,
ADD COLUMN IF NOT EXISTS max_attempts INT NOT NULL DEFAULT 5,
ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

CREATE INDEX IF NOT EXISTS idx_conv_executions_orphan 
ON conversation_executions (status, expires_at)
WHERE status = 'RUNNING';
