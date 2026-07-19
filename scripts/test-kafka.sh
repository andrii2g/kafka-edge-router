#!/usr/bin/env bash
set -euo pipefail

cleanup() {
  status=$?
  if ((status != 0)); then
    docker compose logs kafka >&2
  fi
  docker compose down --volumes
  return "$status"
}
trap cleanup EXIT

docker compose up -d kafka
./scripts/wait-for-kafka.sh
KAFKA_TEST_BROKERS=localhost:9092 \
KAFKA_INTEGRATION_REQUIRED=1 \
  cargo test --locked --test kafka_integration -- --test-threads=1

KAFKA_TEST_BROKERS=localhost:9092 \
KAFKA_INTEGRATION_REQUIRED=1 \
  cargo test --locked -p router-webhook durable::kafka_tests -- --test-threads=1
