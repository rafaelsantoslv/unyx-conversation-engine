-- Migration: 000015_add_resumption_watermark_to_conversations.sql
-- Adiciona marca d'água de retomada (resumed_at_sequence) às conversas (INV-04, INV-06, UDS-202)

ALTER TABLE conversations
ADD COLUMN IF NOT EXISTS resumed_at_sequence BIGINT NOT NULL DEFAULT 0;
