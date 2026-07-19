#!/usr/bin/env bash
set -euo pipefail

DURATION_SECS="${SOAK_DURATION_SECS:-14400}"
PHASE_SECS="${SOAK_PHASE_SECS:-300}"
RATE="${SOAK_RATE_PER_SECOND:-500}"
REBALANCE_EVERY_PHASES="${SOAK_REBALANCE_EVERY_PHASES:-6}"
ROLLOUT_EVERY_PHASES="${SOAK_ROLLOUT_EVERY_PHASES:-8}"
MAX_DURATION_SECS=86400
MAX_PHASE_SECS=3600

require_uint() {
  local name="$1" value="$2" maximum="$3"
  [[ "$value" =~ ^[1-9][0-9]*$ ]] || { echo "$name must be a positive integer" >&2; exit 2; }
  (( value <= maximum )) || { echo "$name must not exceed $maximum" >&2; exit 2; }
}
require_uint SOAK_DURATION_SECS "$DURATION_SECS" "$MAX_DURATION_SECS"
require_uint SOAK_PHASE_SECS "$PHASE_SECS" "$MAX_PHASE_SECS"
require_uint SOAK_RATE_PER_SECOND "$RATE" 100000
require_uint SOAK_REBALANCE_EVERY_PHASES "$REBALANCE_EVERY_PHASES" 10000
require_uint SOAK_ROLLOUT_EVERY_PHASES "$ROLLOUT_EVERY_PHASES" 10000

MESSAGES=$((RATE * PHASE_SECS))
(( MESSAGES <= 10000000 )) || { echo "phase message count exceeds router-load cap" >&2; exit 2; }
PHASES=$(((DURATION_SECS + PHASE_SECS - 1) / PHASE_SECS))
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUTPUT_ROOT="${SOAK_OUTPUT_ROOT:-artifacts/soak/$STAMP}"
mkdir -p "$OUTPUT_ROOT"

{
  echo "started_utc=$STAMP"
  echo "git_commit=$(git rev-parse HEAD)"
  echo "rustc=$(rustc --version)"
  echo "kernel=$(uname -a)"
  echo "logical_cpus=$(getconf _NPROCESSORS_ONLN)"
  echo "duration_secs=$DURATION_SECS"
  echo "phase_secs=$PHASE_SECS"
  echo "rate_per_second=$RATE"
  echo "phases=$PHASES"
} > "$OUTPUT_ROOT/metadata.txt"

load_args=(
  --http-base "${ROUTER_LOAD_HTTP_BASE:-http://127.0.0.1:8080}"
  --websocket-url "${ROUTER_LOAD_WEBSOCKET_URL:-ws://127.0.0.1:8080/v1/ws}"
  --grpc-endpoint "${ROUTER_LOAD_GRPC_ENDPOINT:-http://127.0.0.1:9090}"
  --tenant "${ROUTER_LOAD_TENANT:-tenant-demo}"
  --channel "${ROUTER_LOAD_CHANNEL:-load}"
  --messages "$MESSAGES"
  --rate-per-second "$RATE"
  --websocket-connections "${SOAK_WS_CONNECTIONS:-8}"
  --sse-connections "${SOAK_SSE_CONNECTIONS:-8}"
  --grpc-connections "${SOAK_GRPC_CONNECTIONS:-8}"
  --drain-timeout-secs "${SOAK_DRAIN_TIMEOUT_SECS:-60}"
)
if [[ -n "${ROUTER_LOAD_BEARER_TOKEN:-}" ]]; then
  load_args+=(--bearer-token "$ROUTER_LOAD_BEARER_TOKEN")
fi
if [[ -n "${SOAK_WEBHOOK_LISTEN:-}" ]]; then
  load_args+=(
    --webhook-listen "$SOAK_WEBHOOK_LISTEN"
    --expected-webhooks-per-message "${SOAK_EXPECTED_WEBHOOKS_PER_MESSAGE:-1}"
    --webhook-fail-every "${SOAK_WEBHOOK_FAIL_EVERY:-5}"
  )
fi

sample_resources() {
  local phase="$1"
  if [[ -n "${ROUTER_PID:-}" ]] && kill -0 "$ROUTER_PID" 2>/dev/null; then
    ps -o pid=,rss=,vsz=,%cpu=,etimes= -p "$ROUTER_PID" >> "$OUTPUT_ROOT/process-samples.txt"
  fi
  if [[ -n "${SOAK_KUBECONFIG:-}" ]]; then
    kubectl --kubeconfig "$SOAK_KUBECONFIG" -n "${SOAK_NAMESPACE:-kafka-router-rc}" \
      top pods --containers >> "$OUTPUT_ROOT/kubernetes-samples.txt" 2>&1 || true
  fi
  echo "sampled phase $phase" >> "$OUTPUT_ROOT/events.log"
}

for ((phase = 1; phase <= PHASES; phase++)); do
  phase_args=("${load_args[@]}")
  if (( phase % 3 == 0 )); then
    phase_args+=(--slow-reader-delay-ms "${SOAK_SLOW_READER_DELAY_MS:-25}")
  fi
  cargo run --locked --release -p router-load -- \
    "${phase_args[@]}" --output "$OUTPUT_ROOT/phase-$phase.json"
  sample_resources "$phase"

  if (( phase % REBALANCE_EVERY_PHASES == 0 )) && [[ "${SOAK_RESTART_KAFKA:-false}" == "true" ]]; then
    docker compose restart kafka
    echo "kafka restart after phase $phase" >> "$OUTPUT_ROOT/events.log"
  fi
  if (( phase % ROLLOUT_EVERY_PHASES == 0 )) && [[ -n "${SOAK_KUBECONFIG:-}" ]]; then
    kubectl --kubeconfig "$SOAK_KUBECONFIG" -n "${SOAK_NAMESPACE:-kafka-router-rc}" \
      rollout restart deployment/kafka-edge-router
    kubectl --kubeconfig "$SOAK_KUBECONFIG" -n "${SOAK_NAMESPACE:-kafka-router-rc}" \
      rollout status deployment/kafka-edge-router --timeout=10m
    echo "router rollout after phase $phase" >> "$OUTPUT_ROOT/events.log"
  fi
done

python scripts/summarize-load.py "$OUTPUT_ROOT" > "$OUTPUT_ROOT/summary.json"
echo "soak artifacts: $OUTPUT_ROOT"