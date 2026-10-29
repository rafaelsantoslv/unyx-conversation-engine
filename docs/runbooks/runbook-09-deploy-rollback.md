# RUNBOOK-09: Procedimento de Deploy Rolling Update e Rollback Imediato

## 1. Procedimento de Deploy Zero-Downtime
1. Construir e validar novas imagens com testes íntegros:
   ```bash
   make build test
   ```
2. Aplicar migrações do PostgreSQL (garantindo retrocompatibilidade / Non-Breaking):
   ```bash
   # As migrações rodam automaticamente nos binários na inicialização
   ```
3. Realizar o rolling update container por container:
   ```bash
   docker compose up -d --no-deps --build api
   docker compose up -d --no-deps --build publisher
   docker compose up -d --no-deps --build worker
   docker compose up -d --no-deps --build outbound-worker
   ```
4. O Graceful Shutdown de 15 segundos drena as tarefas ativas antes da substituição dos containers.

## 2. Procedimento de Rollback Imediato
1. Se a nova versão apresentar regressões de SLA ou erros 5xx:
   ```bash
   git checkout <LAST_KNOWN_STABLE_TAG>
   make build
   docker compose up -d --build
   ```
2. Reverter migrações de schema se necessário utilizando os scripts `.down.sql` correspondentes em `migrations/`.
