SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

.PHONY: help fmt lint test test-kafka check build run kafka-up kafka-down topic smoke validate docker bench load soak

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fmt: ## Format all Rust crates
	cargo fmt --all

lint: ## Run Clippy with warnings denied
	cargo clippy --locked --workspace --all-targets --all-features -- -D warnings

test: ## Run the workspace test suite
	cargo test --locked --workspace --all-features

test-kafka: ## Run isolated broker-backed Kafka integration tests
	./scripts/test-kafka.sh

check: ## Type-check every target
	cargo check --locked --workspace --all-targets --all-features

build: ## Build the release daemon
	cargo build --locked --release --bin routerd

run: ## Run the local configuration
	cargo run --locked -p routerd -- --config config/router.toml

kafka-up: ## Start local Kafka
	docker compose up -d kafka

kafka-down: ## Stop local Kafka
	docker compose down

topic: ## Create the local input topic
	./scripts/create-topics.sh

smoke: ## Run the HTTP/SSE smoke test against a running daemon
	./scripts/smoke-test.sh

validate: ## Run repository checks available without a Rust toolchain
	python scripts/validate-repo.py

docker: ## Build the runtime container
	docker build -t kafka-edge-router:local .
bench: ## Run matcher and bounded-dispatch benchmarks
	cargo bench --locked -p router-core --bench matcher

load: ## Run the bounded end-to-end load generator
	cargo run --locked --release -p router-load -- --help

soak: ## Run the four-hour fault-injection soak
	./scripts/soak-test.sh