#!/usr/bin/env bash
set -euo pipefail

docker compose up -d kafka
./scripts/wait-for-kafka.sh
./scripts/create-topics.sh
cat <<'MSG'
Kafka is ready. Start the daemon with:
  cargo run -p routerd -- --config config/router.toml
MSG
