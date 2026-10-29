# RUNBOOK-07: Entregas Ambíguas Acumuladas (Auditoria e Reconciliação Manual)

## 1. Sintomas
- Intenções com status `UNKNOWN` acumuladas após timeouts de rede (Problema dos Dois Generais / UDS-214).

## 2. Diagnóstico
```sql
SELECT id, tenant_id, conversation_id, execution_id, attempts, updated_at
FROM outbound_intents
WHERE status = 'UNKNOWN'
ORDER BY updated_at ASC;
```

## 3. Resolução
1. O `outbound.WorkerService` executa reconciliação ativa no ciclo seguinte chamando `ReconcileAmbiguousIntent`.
2. Se a mensagem já existir no Chatwoot (verificada por echo_id/correlation_id), é marcada como `DELIVERED` sem reenvio.
3. Se a mensagem não tiver chegado ao Chatwoot, o worker tenta o reenvio normalmente.
4. Para forçar resolução manual caso o Chatwoot tenha sido migrado:
   ```sql
   UPDATE outbound_intents SET status = 'DELIVERED', updated_at = NOW() WHERE id = '<INTENT_ID>';
   ```
