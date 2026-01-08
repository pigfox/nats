# Makefile
.PHONY: up down test cover tidy

up:
	docker compose up -d

down:
	docker compose down -v

tidy:
	go mod tidy

test: up
	./scripts/test.sh

cover: up
	NATS_URL=nats://127.0.0.1:4222 go test ./... -count=1 -race -coverprofile=coverage.out
	go tool cover -func=coverage.out | tail -n 20
