# RUNBOOK-02: Outbox Travada / Backlog Acumulado (Purge e Re-enfileiramento)

## 1. Sintomas e Alertas
- Alerta: `OutboxStuck` (Backlog de `outbox_events` pendentes por mais de 3 minutos sem redução).
- Dashboard: Grafana -> *Pipeline & Ingestion* -> Gauge *Outbox Pending Events* em vermelho.

## 2. Ações de Diagnóstico
```bash
# 1. Contar eventos pendentes no PostgreSQL
docker compose exec postgres psql -U postgres -d unyx_conversation -c "
SELECT status, count(*), min(created_at)
FROM outbox_events
GROUP BY status;
"

# 2. Inspecionar logs do Outbox Publisher
docker compose logs --tail=100 publisher
```

## 3. Resolução
1. Verificar se o processo `cmd/publisher` está ativo e saudável:
   ```bash
   curl -s http://localhost:8082/healthz
   ```
2. Caso o publisher tenha entrado em deadlock, reinicie o container:
   ```bash
   docker compose restart publisher
   ```
3. Se houver eventos marcados com status corrompido, resetar para PENDING:
   ```sql
   UPDATE outbox_events SET status = 'PENDING' WHERE status = 'FAILED' AND retry_count < 10;
   ```
