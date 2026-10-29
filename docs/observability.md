# Estratégia de Observabilidade e Telemetria (EPIC-06 / OPS-01)

Este documento formaliza as convenções, padrões e contratos de observabilidade do **Conversation Engine**, baseados nos padrões da CNCF (Cloud Native Computing Foundation) e na especificação **OpenTelemetry (OTel)**.

---

## 1. Nomes Oficiais de Serviços (`service.name`)

| Serviço | Binário | Descrição |
| :--- | :--- | :--- |
| `unyx-conversation-api` | `cmd/api` | API REST de Ingestão e Webhooks Inbound |
| `unyx-outbox-publisher` | `cmd/publisher` | Processo de publicação transacional Outbox → Kafka |
| `unyx-conversation-worker` | `cmd/worker` | Worker assíncrono de orquestração e execução de IA |
| `unyx-outbound-worker` | `cmd/outbound-worker` | Worker de despacho e reconciliação externa (Chatwoot) |

---

## 2. Atributos Semânticos Canônicos (Spans e Logs)

Para garantir rastreabilidade de ponta a ponta sem ambiguidade:

- `tenant.id`: Identificador da organização SaaS (multi-tenant isolation).
- `conversation.id`: UUID soberano da conversa interna.
- `message.id`: UUID da mensagem em processamento.
- `execution.id`: UUID da execução da IA / Intent de saída.
- `message.sequence`: Sequence number BIGINT estrito da mensagem.
- `messaging.channel`: Canal canônico (`whatsapp`, `livechat`, etc.).
- `execution.status`: Status da operação (`SUCCEEDED`, `FAILED`, `CANCELLED`).
- `error.type`: Categoria de erro (`TRANSIENT`, `PERMANENT`, `TIMEOUT`).

---

## 3. Propagação de Contexto Distribuído (W3C Trace Context)

Toda comunicação entre serviços do Engine (HTTP → Postgres → Kafka → Worker → Outbound) propaga o padrão **W3C Trace Context**:
- Header HTTP: `traceparent` e `tracestate`.
- Headers Kafka: `traceparent` codificado nos metadados de cada mensagem.
- Spans filhos mantêm o mesmo `TraceID` raiz, permitindo inspecionar a jornada completa de uma mensagem desde o clique do cliente no WhatsApp até a entrega da resposta.

---

## 4. Regras de Cardinalidade em Métricas Prometheus

> [!IMPORTANT]
> **Proibido em Labels de Métricas Prometheus:**
> `conversation_id`, `message_id`, `external_id`, `content` e qualquer identificador com cardinalidade irrestrita.
> Labels permitidos em métricas Prometheus:
> `tenant_id` (para billing/SLOs por tenant), `channel`, `status`, `error_type`, `model`.

---

## 5. Política de Sanitização e Redação de Dados (INV-02)

Zero vazamento de credenciais ou dados sensíveis:
1. Headers `api_access_token`, `X-Chatwoot-Webhook-Token`, `Authorization: Bearer` são automaticamente mascarados como `[REDACTED_CREDENTIALS]`.
2. Payloads de mensagens do usuário não são expostos em spans de trace globais (apenas `len` e hash).
