BIN := bin/walcast
PKG := ./cmd/walcast

-include .env
export

.DEFAULT_GOAL := build
.PHONY: run build test test-integration bench bench-e2e fuzz vuln lint fmt up down

run:
	go run $(PKG)

build:
	go build -o $(BIN) $(PKG)

test:
	go test -race -shuffle=on ./...

test-integration:
	go test -tags integration -race -shuffle=on -count=1 ./internal/replication/

bench-e2e:
	go test -tags integration -run '^$$' -bench BenchmarkEndToEnd -benchtime=1x -count=3 -timeout 30m ./internal/replication

bench:
	go test -run '^$$' -bench . -benchmem -count=6 ./... | tee bench.txt

fuzz:
	go test -run '^$$' -fuzz FuzzAppendString -fuzztime 30s ./internal/event
	go test -run '^$$' -fuzz FuzzAppendValueJSON -fuzztime 30s ./internal/event

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

lint:
	golangci-lint run

fmt:
	golangci-lint fmt

up:
	docker compose up -d --wait

down:
	docker compose down
