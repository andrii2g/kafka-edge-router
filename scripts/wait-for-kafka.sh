#!/usr/bin/env bash
set -euo pipefail

for _ in $(seq 1 60); do
  if docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    exit 0
  fi
  sleep 1
done

echo "Kafka did not become ready" >&2
exit 1
