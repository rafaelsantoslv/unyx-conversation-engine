.PHONY: all up down logs ps clean test test-e2e build run-api run-worker run-publisher

all: test build

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

build:
	go build -buildvcs=false ./cmd/...

test:
	go test -v ./...

test-e2e:
	go test -v ./tests/e2e/...

test-load:
	docker run --rm -i --net=host -v $(shell pwd)/tests/load:/tests/load grafana/k6 run /tests/load/ingestion_stress.js

test-resilience:
	go test -v ./tests/e2e/... -run TestResilience_EPIC03_Matrix

run-api:
	go run -buildvcs=false ./cmd/api

run-worker:
	go run -buildvcs=false ./cmd/worker

run-publisher:
	go run -buildvcs=false ./cmd/publisher
