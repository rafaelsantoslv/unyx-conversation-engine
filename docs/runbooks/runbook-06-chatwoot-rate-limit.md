# RUNBOOK-06: Chatwoot Offline / HTTP 429 Persistente (Backpressure e Pausa Temporária do Outbound)

## 1. Sintomas e Alertas
- Alerta: `ChatwootRateLimited` ou `OutboundDeliveryFailing`.
- Dashboard: Grafana -> *Outbound Delivery* -> Taxa de HTTP 429 ou 5xx elevada.

## 2. Diagnóstico
```bash
# Consultar métricas do outbound worker
curl -s http://localhost:8083/metrics | grep outbound_intents_total
```

## 3. Resolução
1. O `outbound.WorkerService` possui backoff exponencial nativo com jitter (`retry.Backoff`).
2. Se o rate limit do provedor for severo, aumente temporariamente o poll interval do worker:
   - Configuração de ambiente: `OUTBOUND_POLL_INTERVAL=500ms` ou reduza o `OUTBOUND_BATCH_SIZE=5`.
3. As intenções pendentes permanecem em segurança na tabela `outbound_intents` até o alívio do rate limit.
