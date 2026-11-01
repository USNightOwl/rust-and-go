.PHONY: up down build test lint run-api run-payment run-notification

up:            ## Dựng toàn bộ hệ thống bằng Docker Compose
	docker compose up -d --build

down:
	docker compose down -v

build:
	go build ./...

test:          ## Unit test + test với kfake (không cần Docker)
	go test -race -count=1 ./...

lint:
	go vet ./...

run-api:       ## Chạy từng dịch vụ trên máy (Kafka trong Docker ở localhost:9092)
	go run ./cmd/order-api

run-payment:
	go run ./cmd/payment-worker

run-notification:
	go run ./cmd/notification-worker
