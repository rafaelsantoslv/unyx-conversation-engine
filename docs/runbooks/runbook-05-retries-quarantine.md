# RUNBOOK-05: Taxa de Retries Anormal / Quarentena Cheia (Inspeção e Reprocessamento de Poison Pills)

## 1. Sintomas e Alertas
- Alerta: `QuarantineEventsSpike` ou `HighRetryRate`.
- Dashboard: Grafana -> *Worker & Execution* -> Gráfico de Quarentena em ascensão.

## 2. Diagnóstico
```sql
SELECT id, tenant_id, conversation_id, reason, error_details, created_at
FROM quarantine_events
ORDER BY created_at DESC
LIMIT 20;
```

## 3. Resolução
1. Identificar se o erro é decorrente de formato inválido (ex: payload sem campos obrigatórios) ou instabilidade de LLM.
2. Se o problema tiver sido corrigido no código (ex: deploy de novo parser):
   ```sql
   -- Reprocessar eventos em quarentena movendo de volta para outbox_events
   INSERT INTO outbox_events (id, tenant_id, conversation_id, event_type, payload, partition_key, status, retry_count, created_at)
   SELECT gen_random_uuid(), tenant_id, conversation_id, 'conversation.message.received', raw_payload, conversation_id, 'PENDING', 0, NOW()
   FROM quarantine_events
   WHERE resolved_at IS NULL;

   UPDATE quarantine_events SET resolved_at = NOW() WHERE resolved_at IS NULL;
   ```
