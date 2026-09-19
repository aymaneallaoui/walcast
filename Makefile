BIN := bin/walcast
PKG := ./cmd/walcast

.DEFAULT_GOAL := build
.PHONY: run build test lint fmt up down

run:
	go run $(PKG)

build:
	go build -o $(BIN) $(PKG)

test:
	go test -race ./...

lint:
	golangci-lint run

fmt:
	golangci-lint fmt

up:
	docker compose up -d --wait

down:
	docker compose down
