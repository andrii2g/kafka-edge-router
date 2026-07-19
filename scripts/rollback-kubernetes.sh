#!/usr/bin/env bash
set -euo pipefail

KUBECONFIG_PATH="${1:-}"
NAMESPACE="${2:-}"
DIGEST="${3:-}"
DEPLOYMENT="${4:-kafka-edge-router}"
IMAGE="${KAFKA_EDGE_ROUTER_IMAGE:-ghcr.io/andrii2g/kafka-edge-router}"

[[ -f "$KUBECONFIG_PATH" ]] || { echo "usage: $0 KUBECONFIG NAMESPACE SHA256_DIGEST [DEPLOYMENT]" >&2; exit 2; }
[[ "$NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || { echo "invalid namespace" >&2; exit 2; }
[[ "$DEPLOYMENT" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || { echo "invalid deployment" >&2; exit 2; }
[[ "$DIGEST" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "digest must be sha256 followed by 64 lowercase hex characters" >&2; exit 2; }

TARGET="${IMAGE}@${DIGEST}"
echo "rolling back ${NAMESPACE}/${DEPLOYMENT} to immutable image ${TARGET}"
kubectl --kubeconfig "$KUBECONFIG_PATH" -n "$NAMESPACE" \
  set image "deployment/${DEPLOYMENT}" "router=${TARGET}"
kubectl --kubeconfig "$KUBECONFIG_PATH" -n "$NAMESPACE" \
  rollout status "deployment/${DEPLOYMENT}" --timeout=10m
ACTUAL="$(kubectl --kubeconfig "$KUBECONFIG_PATH" -n "$NAMESPACE" \
  get "deployment/${DEPLOYMENT}" -o jsonpath='{.spec.template.spec.containers[?(@.name=="router")].image}')"
[[ "$ACTUAL" == "$TARGET" ]] || { echo "rollback verification failed: $ACTUAL" >&2; exit 1; }
echo "rollback verified: ${TARGET}"