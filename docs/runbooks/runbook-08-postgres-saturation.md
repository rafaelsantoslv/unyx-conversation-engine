# RUNBOOK-08: PostgreSQL Indisponível / Saturação de Conexões (Pool Tuning e Failover)

## 1. Sintomas e Alertas
- Alerta: `PostgresPoolExhaustion`.
- Erros nos logs: `too many connections for role postgres` ou timeouts no `pgxpool.Acquire`.

## 2. Diagnóstico
```sql
-- Verificar conexões abertas por estado e aplicação
SELECT application_name, state, count(*)
FROM pg_stat_activity
GROUP BY application_name, state;
```

## 3. Resolução
1. Encerrar conexões ociosas ou presas:
   ```sql
   SELECT pg_terminate_backend(pid)
   FROM pg_stat_activity
   WHERE state = 'idle in transaction' AND state_change < NOW() - INTERVAL '30 seconds';
   ```
2. Aumentar `max_connections` no `postgresql.conf` ou ajustar os parâmetros `pool_max_conns` nos serviços do Engine.
3. Em clusters de alta densidade, intercalar o **PgBouncer** com pool de transações.
