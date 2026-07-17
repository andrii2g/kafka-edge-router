#!/usr/bin/env bash
set -euo pipefail

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic router.input \
  --partitions "${ROUTER_TOPIC_PARTITIONS:-6}" \
  --replication-factor 1
