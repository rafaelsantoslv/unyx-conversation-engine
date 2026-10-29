# unyx-conversation-engine

Engine de conversação e eventos do ecossistema Unyx Chat.

---

## 🚀 Pré-requisitos

- [Go](https://go.dev/) (>= 1.24 ou versão especificada no `go.mod`)
- [Docker](https://www.docker.com/) e [Docker Compose](https://docs.docker.com/compose/)
- [Make](https://www.gnu.org/software/make/) (opcional, para automação dos comandos)

---

## 🐳 Infraestrutura com Docker Compose

O repositório inclui um arquivo [`docker-compose.yml`](file:///home/rafael/Desktop/projetos/unyx-chat/unyx-conversation-engine/docker-compose.yml) configurado com:

1. **PostgreSQL 16 Alpine (`postgres:16-alpine`)**:
   - Porta mapeada: `5432:5432`
   - Healthcheck configurado via `pg_isready`
   - Banco padrão: `unyx_conversation`
   - Credenciais padrão: `postgres`/`postgres`

2. **Apache Kafka (KRaft Mode)**:
   - Sem dependência do ZooKeeper (modo KRaft nativo)
   - Listener interno (entre containers): `PLAINTEXT://kafka:29092`
   - Listener externo (host/localhost): `EXTERNAL://localhost:9092`

---

## ⚙️ Variáveis de Ambiente

As configurações são gerenciadas pelo pacote [`internal/config`](file:///home/rafael/Desktop/projetos/unyx-chat/unyx-conversation-engine/internal/config/config.go). Um arquivo base [`env.example`](file:///home/rafael/Desktop/projetos/unyx-chat/unyx-conversation-engine/.env.example) está disponível:

| Variável | Descrição | Valor Padrão |
|---|---|---|
| `PORT` | Porta HTTP do serviço | `8080` (api), `8081` (worker), `8082` (publisher) |
| `DATABASE_URL` | URL de conexão PostgreSQL | `postgres://postgres:postgres@localhost:5432/unyx_conversation?sslmode=disable` |
| `KAFKA_BROKERS` | Lista de brokers Kafka separados por vírgula | `localhost:9092` |

---

## 🛠️ Comandos do Makefile

| Comando | Descrição |
|---|---|
| `make up` | Sobe os containers (PostgreSQL e Kafka) em segundo plano |
| `make down` | Para e remove os containers da stack |
| `make logs` | Acompanha os logs dos containers em tempo real |
| `make ps` | Exibe o status e healthcheck dos containers |
| `make build` | Compila todos os binários em `cmd/` |
| `make test` | Executa os testes automatizados da aplicação |
| `make test-e2e` | Executa os testes de integração ponta a ponta (E2E) |
| `make run-api` | Inicia o executável da API HTTP (`cmd/api`) |
| `make run-worker` | Inicia o executável do Worker (`cmd/worker`) |
| `make run-publisher`| Inicia o executável do Publisher (`cmd/publisher`) |

---

## 🩺 Endpoints de Health Check

Cada executável (`api`, `worker`, `publisher`) expõe endpoints HTTP nativos utilizando `net/http`:

- **`GET /healthz`**: Verifica integridade do serviço.
  - Resposta: `200 OK`
  - Body: `{"status":"ok","service":"<nome-do-servico>"}`
- **`GET /livez`**: Verifica liveness do processo.
  - Resposta: `200 OK`
  - Body: `{"status":"alive","service":"<nome-do-servico>"}`

### Exemplos de teste rápido com `curl`:

```bash
# API (porta padrão 8080)
curl http://localhost:8080/healthz
curl http://localhost:8080/livez

# Worker (porta padrão 8081)
curl http://localhost:8081/healthz
curl http://localhost:8081/livez

# Publisher (porta padrão 8082)
curl http://localhost:8082/healthz
curl http://localhost:8082/livez
```

---

## 📨 Ingestion API (`POST /v1/messages`)

Recebe mensagens de webhooks externos com processamento assíncrono (**`202 Accepted`**), protegida contra payloads abusivos (>1MB) e com injeção automática de `X-Request-ID`.

### Exemplo de Ingestão:

```bash
curl -i -X POST http://localhost:8080/v1/messages \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "tenant-123",
    "conversation_id": "conv-456",
    "external_message_id": "wpp-789",
    "channel": "whatsapp",
    "role": "user",
    "content": "Olá, preciso de ajuda!"
  }'
```

**Resposta:**
```http
HTTP/1.1 202 Accepted
Content-Type: application/json
X-Request-Id: 44f2d34a413d9ea0c43666b3d4111fa6

{
  "status": "accepted",
  "message_id": "fa2c4bb41e97deff8beceee4d4f8ff93",
  "conversation_id": "conv-456",
  "accepted_at": "2026-10-27T23:42:00Z"
}
```



---

## 🧪 Testes Ponta a Ponta (End-to-End E2E)

O repositório possui uma suíte automatizada de testes E2E ([`tests/e2e/e2e_test.go`](file:///home/rafael/Desktop/projetos/unyx-chat/unyx-conversation-engine/tests/e2e/e2e_test.go)) que comprova a orquestração completa em ambiente distribuído real (PostgreSQL + Apache Kafka):

1. **Caminho Feliz (Happy Path)**: `POST /v1/messages` (202 Accepted) ➡️ PostgreSQL (`messages` + `outbox_events` PENDING) ➡️ Polling Publisher ➡️ Apache Kafka (`conversation.events`) ➡️ Conversation Worker ➡️ Mock Runtime (IA) ➡️ Persistência Durável (`processing_results` SUCCEEDED) ➡️ Commit manual de offset no Kafka.
2. **Payload Inválido**: Validação precoce com retorno `422 Unprocessable Entity` sem poluir as tabelas relacionais.
3. **Deduplicação / Idempotência**: Envio repetido do mesmo `external_message_id` para o mesmo tenant; o sistema identifica a duplicidade e processa o evento apenas uma vez.
4. **Tratamento de Falhas no Runtime**: Injeção controlada de falha na IA/Mock; o sistema não entra em pânico e registra o status `FAILED` com a mensagem de erro estruturada para auditoria.
5. **Resiliência a Reinicialização**: Simulação de queda do Worker com mensagens pendentes na partição; o Worker é reinicializado com o mesmo Consumer Group e retoma o processamento sem perda de dados.

### Como rodar os testes E2E:

Certifique-se de que a stack Docker está ativa (`make up`) e execute:

```bash
make test-e2e
```

---

## ⚠️ Limitações Conhecidas da Versão V1 (Core)

Esta versão comprova a fundação assíncrona, desacoplamento e tolerância a falhas do Core. Como base para os próximos épicos, as seguintes limitações de escopo foram adotadas deliberadamente:

1. **Polling Outbox Publisher**: O publisher realiza consultas periódicas (`SKIP LOCKED`) a cada 50-100ms. Para volumes ultra massivos (milhões de msgs/segundo), poderá ser avaliado Change Data Capture (CDC / Debezium).
2. **Ausência de Lease Distribuído Complexo**: O paralelismo por conversa é assegurado pelo particionamento por hash de chave no Kafka (`tenant_id:conversation_id`). Locks distribuídos entre workers concorrentes além do broker não estão incluídos nesta fase.
3. **Agent Runtime Mockado**: O processamento de inteligência artificial utiliza o `MockRuntime` configurável para validar concorrência, latência e falhas de rede sem custos de provedores externos. A integração com APIs de LLMs comerciais ocorrerá no runtime de inferência.
