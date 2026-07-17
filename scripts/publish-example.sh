#!/usr/bin/env bash
set -euo pipefail

endpoint="${ROUTER_HTTP_ENDPOINT:-http://127.0.0.1:8080}"
curl --fail-with-body --silent --show-error \
  -H 'content-type: application/json' \
  -X POST "${endpoint}/v1/publish" \
  --data @examples/publish.json
printf '\n'
