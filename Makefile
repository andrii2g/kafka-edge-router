SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

.PHONY: help fmt lint test check build run kafka-up kafka-down topic smoke validate docker

help: ## Show available targets
	@awk 'BEGIN {FS = ":.*## "; printf "Usage: make <target>\n\n"} /^[a-zA-Z_-]+:.*## / {printf "  %-14s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

fmt: ## Format all Rust crates
	cargo fmt --all

lint: ## Run Clippy with warnings denied
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test: ## Run the workspace test suite
	cargo test --workspace --all-features

check: ## Type-check every target
	cargo check --workspace --all-targets --all-features

build: ## Build the release daemon
	cargo build --release --bin routerd

run: ## Run the local configuration
	cargo run -p routerd -- --config config/router.toml

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
	docker build -t rust-kafka-edge-router:local .
