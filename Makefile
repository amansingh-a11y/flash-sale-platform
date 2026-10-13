.PHONY: test build run up down loadtest k6 diagrams

test:
	go vet ./...
	go test -race -count=1 ./...

build:
	go build -o bin/ ./cmd/...

# Requires a local redis on :6379 and a populated .env
run: build
	set -a && . ./.env && set +a && REDIS_ADDR=localhost:6379 ./bin/reservation-api

up:
	docker compose up --build -d

down:
	docker compose down -v

loadtest: build
	set -a && . ./.env && set +a && ./bin/loadgen -base http://localhost:8080 -stock 2000 -users 30000 -concurrency 300

k6:
	set -a && . ./.env && set +a && k6 run -e BASE=http://localhost:8080 loadtest/k6/spike.js

diagrams:
	cd docs/diagrams && for f in *.mmd; do npx -y @mermaid-js/mermaid-cli -i $$f -o $${f%.mmd}.png -w 2400 -b white; done
