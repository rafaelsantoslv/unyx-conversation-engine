-- Adiciona suporte a status UNKNOWN no índice de polling / reconciliação
CREATE INDEX IF NOT EXISTS idx_outbound_intents_reconciliation
ON outbound_intents(status, next_retry_at, created_at)
WHERE status IN ('PENDING', 'FAILED', 'UNKNOWN');
