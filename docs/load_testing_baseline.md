# Baseline de Capacidade e Testes de Carga (OPS-08)

Este documento registra os resultados empíricos, percentis de latência e limites de saturação do **Unyx Conversation Engine** sob testes sintéticos de carga executados com [k6](https://k6.io).

---

## 1. Topologia e Parâmetros de Teste

- **Ambiente**: Cluster local de referência (Docker Compose em Linux x86_64).
- **Componentes Ativos**:
  - `cmd/api`: Ingestion HTTP API (porta 8080).
  - `cmd/publisher`: Transactional Outbox Publisher polling a cada 100ms.
  - `cmd/worker`: Conversation Worker consumindo eventos particionados do Kafka com Mock Agent Runtime (15ms latência fixa).
  - `cmd/outbound-worker`: Outbound Worker despachando intents para mock HTTP.
  - `PostgreSQL 16`: Pool máximo de conexões = 50.
  - `Apache Kafka 3.8.0 (KRaft)`: Tópico `conversation.events` com 6 partições.

---

## 2. Cenários Executados

### Cenário A: Ingestion Stress & Spike (`tests/load/ingestion_stress.js`)
- **Baseline Sustentado**: 50 RPS contínuos durante 10 minutos (30.000 mensagens totais).
- **Spike**: Salto repentino de 50 para 200 RPS em 30 segundos, sustentado por 1 minuto.

### Cenário B: End-to-End Chatwoot Pipeline (`tests/load/end_to_end_pipeline.js`)
- Webhook Inbound Chatwoot -> Deduplicação -> Outbox Postgres -> Kafka Broker -> Worker Pipeline (FSM & Snapshot) -> Outbound Intent -> Outbound Worker.
- Ramping até 100 RPS simultâneos.

---

## 3. Resultados Consolidados de Métricas (SLIs / SLOs)

| Métrica | Target (SLA) | Resultado Medido (Baseline 50 RPS) | Resultado no Spike (200 RPS) | Status |
| :--- | :--- | :--- | :--- | :--- |
| **HTTP Request Failed Rate** | `< 0.1%` | **0.00%** (0 erros) | **0.02%** | **APROVADO** |
| **Ingestion P50 (Mediana)** | `< 50ms` | **12.4ms** | **28.1ms** | **APROVADO** |
| **Ingestion P95** | `< 200ms` | **34.8ms** | **89.5ms** | **APROVADO** |
| **Ingestion P99** | `< 500ms` | **72.1ms** | **145.2ms** | **APROVADO** |
| **Outbox Dispatch Latency P95** | `< 1.0s` | **180ms** | **450ms** | **APROVADO** |
| **Kafka Consumer Lag Residual** | `0 (drenado)` | **0 mensagens** | **0 mensagens** (drenado em 8s) | **APROVADO** |
| **Goroutine / Memory Leaks** | `Nenhum` | **Estável em 48MB RSS** | **Estável em 65MB RSS** | **APROVADO** |

---

## 4. Análise de Ponto de Ruptura e Gargalos Identificados

1. **Primeiro Gargalo Atingido**: Pool de Conexões do PostgreSQL (`pgxpool.MaxConns = 50`).
   - Ao ultrapassar 350 RPS simultâneos na Ingestion API com batch insertions individuais, o tempo de aquisição de conexão no pool sobe para > 300ms.
   - **Mitigação Recomendada**: Utilizar PgBouncer em modo transaction pooling em ambientes de produção com > 500 RPS.
2. **Kafka Broker**: Com 6 partições e replicação fator 1 local, o throughput máximo medido foi de ~1.200 msgs/seg sem apresentar lag acumulado.
3. **Imutabilidade de Entrada e FSM**: Durante todo o teste de 200 RPS, zero condições de corrida ou violações de lock otimista (`INV-02`, `INV-04`, `INV-06`) foram registradas.
