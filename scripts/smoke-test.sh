#!/usr/bin/env bash
set -euo pipefail

endpoint="${ROUTER_HTTP_ENDPOINT:-http://127.0.0.1:8080}"

echo "Checking liveness..."
curl --fail-with-body --silent "${endpoint}/health/live" | grep -q 'live'

echo "Checking readiness..."
curl --fail-with-body --silent "${endpoint}/health/ready" | grep -q 'ready'

echo "Publishing example..."
./scripts/publish-example.sh >/dev/null

echo "Checking metrics..."
curl --fail-with-body --silent "${endpoint}/metrics" | grep -q 'router_kafka_messages_total'

echo "Smoke test passed"
