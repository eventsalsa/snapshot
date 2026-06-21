.PHONY: help test test-unit test-integration lint fmt build check govulncheck

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'

check: lint govulncheck test-unit test-integration ## Run all checks (lint, govulncheck, unit tests, integration tests)

test: test-unit ## Run all tests

test-unit: ## Run unit tests
	go test -v -race -coverprofile=coverage.out ./...

test-integration: ## Run integration tests (requires Docker daemon for testcontainers)
	go test -p 1 -v -race -tags=integration ./...

lint: ## Run linter
	golangci-lint run --timeout=5m

govulncheck: ## Run vulnerability checker
	govulncheck ./...

fmt: ## Format code
	gofmt -w -s .
	goimports -w -local github.com/eventsalsa/snapshot .

build: ## Build all packages
	go build -v ./...
