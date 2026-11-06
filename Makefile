DATABASE_URL ?= postgres://durableq:durableq@localhost:5432/durableq?sslmode=disable
export DATABASE_URL

.PHONY: up down migrate test vet chaos demo

up:
	docker compose up -d --wait postgres

down:
	docker compose down

migrate:
	go run ./cmd/durableq migrate

vet:
	go vet ./...

test:
	go test -race ./...

chaos:
	go test -tags chaos -timeout 10m ./chaos/...

demo:
	docker compose run --rm demo
