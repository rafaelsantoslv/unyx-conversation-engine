# RUNBOOK-04: Execuções Presas / Leases Órfãos (Execução manual do Reconciliador)

## 1. Sintomas
- Conversas em estado de processamento eterno sem resposta ao cliente no WhatsApp.
- Execuções na tabela `conversation_executions` com `status = 'ACTIVE'` e `lease_expires_at < NOW()`.

## 2. Diagnóstico
```sql
SELECT id, tenant_id, conversation_id, worker_id, fencing_token, lease_expires_at
FROM conversation_executions
WHERE status = 'ACTIVE' AND lease_expires_at < NOW();
```

## 3. Resolução
1. O `worker.ExecutionReconciler` roda a cada 15s automaticamente em background e recupera o lease incrementando o `fencing_token`.
2. Para forçar liberação imediata via SQL em emergência:
   ```sql
   UPDATE conversation_executions
   SET status = 'ORPHAN_RECOVERED', updated_at = NOW()
   WHERE status = 'ACTIVE' AND lease_expires_at < NOW() - INTERVAL '1 minute';
   ```
3. O Scheduler re-enfileirará a execução pendente na próxima checagem.
