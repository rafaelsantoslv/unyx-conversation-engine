# RUNBOOK-01: Kafka Indisponível (Degradação e Recuperação de Broker)

## 1. Sintomas e Alertas
- Alerta: `KafkaConsumerLagHigh` ou indisponibilidade de conexão nos logs do publisher/worker (`connection refused` na porta 9092).
- Dashboard: Grafana -> *Pipeline & Ingestion* -> Sem fluxo de mensagens consumidas.

## 2. Ações de Diagnóstico
```bash
# 1. Verificar status dos brokers Kafka
docker compose ps kafka

# 2. Inspecionar logs do broker Kafka
docker compose logs --tail=100 kafka

# 3. Testar conectividade do listener
docker compose exec kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

## 3. Resolução e Mitigação
1. Se o container estiver parado ou morto por OOM, reinicie:
   ```bash
   docker compose restart kafka
   ```
2. Graças ao Transactional Outbox pattern (`INV-01`), as mensagens recebidas pela Ingestion API continuam seguras no PostgreSQL em `outbox_events` durante todo o período de queda do broker.
3. Assim que o Kafka retornar, o `cmd/publisher` reinicia a publicação dos eventos pendentes automaticamente sem perda de dados.
