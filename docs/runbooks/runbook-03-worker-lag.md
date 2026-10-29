# RUNBOOK-03: Conversation Worker sem Progresso / Consumer Lag Alto (Scaling e Rebalance)

## 1. Sintomas e Alertas
- Alerta: `KafkaConsumerLagHigh` ou `ConversationWorkerFrozen`.
- Dashboard: Grafana -> *Worker & Execution* -> Taxa de consumo = 0 com fila crescendo no Kafka.

## 2. Ações de Diagnóstico
```bash
# 1. Medir o lag exato por partição
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group conversation-engine-workers

# 2. Inspecionar logs dos workers
docker compose logs --tail=100 worker
```

## 3. Resolução
1. Escalar réplicas de workers horizontalmente para acelerar o esvaziamento da fila:
   ```bash
   docker compose -f docker-compose.yml -f docker-compose.scale.yml up -d --scale conversation-worker-1=2
   ```
2. Caso um worker esteja bloqueado em chamada externa sem timeout, o Graceful Shutdown com deadline de 15s (`INV-06`) garante encerramento limpo sem corromper commits.
