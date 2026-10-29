# RUNBOOK-10: Procedimento de Backup Físico/Lógico e Restauração do PostgreSQL

## 1. Backup Lógico Automatizado
```bash
# Executa dump completo com compressão
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
docker compose exec postgres pg_dump -U postgres -d unyx_conversation -Fc > backup_unyx_${TIMESTAMP}.dump
```

## 2. Restauração Completa de Base (Disaster Recovery)
```bash
# 1. Parar serviços que escrevem no banco
docker compose stop api publisher worker outbound-worker

# 2. Restaurar dump
docker compose exec -T postgres dropdb -U postgres --if-exists unyx_conversation
docker compose exec -T postgres createdb -U postgres unyx_conversation
docker compose exec -T postgres pg_restore -U postgres -d unyx_conversation < backup_unyx_${TIMESTAMP}.dump

# 3. Reiniciar serviços
docker compose start api publisher worker outbound-worker
```

## 3. RTO / RPO
- **RTO (Recovery Time Objective)**: < 10 minutos para restauração lógica.
- **RPO (Recovery Point Objective)**: < 1 minuto com WAL archiving (Point-In-Time Recovery ativado).
