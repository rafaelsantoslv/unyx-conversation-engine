# Production Readiness Review (PRR) — Go-Live Certification

**Projeto**: Unyx Conversation Engine  
**Versão Homologada**: v1.0.0-rc  
**Data de Homologação**: 2026-10-29  
**Responsável Técnico**: UnyxDev / Architecture & SRE  
**Status**: **APROVADO PARA PRODUÇÃO (GO-LIVE)**

---

## 1. Verificação dos 5 Pilares de Excelência Operacional

### I. Confiabilidade e Invariantes
- [x] **INV-01 (Persistência Individual)**: Todas as mensagens são gravadas individualmente com integridade referencial antes de qualquer disparo.
- [x] **INV-02 (Ordenação e Serialização)**: Particionamento rígido por conversa (`tenant_id:conversation_id`) garante execução estritamente sequencial.
- [x] **INV-03 (Controle de Cardinalidade)**: Telemetria Prometheus e logs estruturados proíbem IDs como labels, prevenindo memory leaks.
- [x] **INV-04 (Imutabilidade de Entrada)**: Snapshot lógico delimitado por watermark protege execuções e retries de drift de contexto.
- [x] **INV-05 (Uma Execução Ativa)**: Locks distribuídos com fencing tokens eliminam execuções zumbis e concorrência sobre a mesma conversa.
- [x] **INV-06 (Respeito ao Estado)**: Máquina de estados FSM bloqueia envios da IA durante atendimento humano ou conversas fechadas.
- [x] **INV-07 (Reconciliação e Resiliência)**: Problema dos Dois Generais mitigado por consulta ativa idempotente ao Chatwoot.
- [x] **INV-08 (Backpressure e Quarentena)**: Poison pills isolados em `quarantine_events` sem travar filas do Kafka ou pools do banco.

### II. Observabilidade e Telemetria
- [x] Rota `/metrics` ativa expondo contadores, gauges e histogramas RED/USE.
- [x] Logs em JSON estruturado com W3C TraceContext (`trace_id`, `span_id`) correlacionados e credenciais redigidas.
- [x] 4 Dashboards operacionais no Grafana provisionados automaticamente (`deploy/grafana/dashboards`).
- [x] 8 Alertas do Alertmanager declarados e validados via `promtool` com links para runbooks.

### III. Resiliência Operacional e Graceful Shutdown
- [x] Probes de Liveness (`/healthz`) desacopladas de dependências externas.
- [x] Probes de Readiness (`/readyz`) testando conectividade ativa com PostgreSQL.
- [x] Manipulação coordenada de `SIGINT`/`SIGTERM` com drenagem de 15 segundos para tarefas in-flight.
- [x] Recuperação de execuções órfãs por workers substitutos através do `ExecutionReconciler`.

### IV. Capacidade e Escalabilidade
- [x] Baseline comprovado via testes k6: 50 RPS contínuos (< 35ms P95) e suporta picos de 200 RPS (< 90ms P95).
- [x] Escala horizontal comprovada com 3 a 5 workers simultâneos distribuindo partições sem contenção.

### V. Segurança e Conformidade
- [x] Validação de assinatura/token de webhooks com rejeição imediata de credenciais inválidas (HTTP 401).
- [x] Isolamento estrito multi-tenant em nível de storage, particionamento e políticas customizadas.
- [x] Redação automática de Bearer Tokens e chaves de API nos logs de produção.

---

## 2. Índice de Runbooks Operacionais

Todos os incidentes operacionais mapeados contam com procedimentos padronizados em `docs/runbooks/`:

1. [RUNBOOK-01: Kafka Indisponível](runbooks/runbook-01-kafka-unreachable.md)
2. [RUNBOOK-02: Outbox Travada / Backlog Acumulado](runbooks/runbook-02-outbox-stuck.md)
3. [RUNBOOK-03: Conversation Worker sem Progresso / Lag Alto](runbooks/runbook-03-worker-lag.md)
4. [RUNBOOK-04: Execuções Presas / Leases Órfãos](runbooks/runbook-04-orphan-executions.md)
5. [RUNBOOK-05: Taxa de Retries Anormal / Quarentena Cheia](runbooks/runbook-05-retries-quarantine.md)
6. [RUNBOOK-06: Chatwoot Offline / HTTP 429 Persistente](runbooks/runbook-06-chatwoot-rate-limit.md)
7. [RUNBOOK-07: Entregas Ambíguas Acumuladas](runbooks/runbook-07-ambiguous-deliveries.md)
8. [RUNBOOK-08: PostgreSQL Indisponível / Saturação de Conexões](runbooks/runbook-08-postgres-saturation.md)
9. [RUNBOOK-09: Deploy Rolling Update e Rollback](runbooks/runbook-09-deploy-rollback.md)
10. [RUNBOOK-10: Backup Físico/Lógico e Restauração PITR](runbooks/runbook-10-backup-recovery.md)

---

## 3. Autorização de Entrada em Produção

Certifico que todos os requisitos do pipeline SDD, invariantes de arquitetura e critérios de aceite foram auditados e atendidos integralmente.
