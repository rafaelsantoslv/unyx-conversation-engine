.PHONY: all up down logs ps clean test run-api run-worker run-publisher

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

run-api:
	go run -buildvcs=false ./cmd/api

run-worker:
	go run -buildvcs=false ./cmd/worker

run-publisher:
	go run -buildvcs=false ./cmd/publisher
