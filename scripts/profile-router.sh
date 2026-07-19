#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-}"
PID="${2:-}"
DURATION_SECS="${3:-60}"
OUTPUT="${4:-artifacts/profiles}"
[[ "$MODE" =~ ^(cpu|allocations|locks|memory)$ ]] || { echo "usage: $0 cpu|allocations|locks|memory PID [seconds] [output]" >&2; exit 2; }
[[ "$PID" =~ ^[1-9][0-9]*$ ]] || { echo "PID must be positive" >&2; exit 2; }
[[ "$DURATION_SECS" =~ ^[1-9][0-9]*$ ]] && (( DURATION_SECS <= 3600 )) || { echo "seconds must be 1..3600" >&2; exit 2; }
kill -0 "$PID"
mkdir -p "$OUTPUT"

case "$MODE" in
  cpu)
    command -v perf >/dev/null || { echo "perf is required" >&2; exit 1; }
    perf record -F 99 -g -p "$PID" -o "$OUTPUT/cpu.data" -- sleep "$DURATION_SECS"
    perf report --stdio -i "$OUTPUT/cpu.data" > "$OUTPUT/cpu.txt"
    ;;
  allocations)
    command -v heaptrack >/dev/null || { echo "heaptrack is required" >&2; exit 1; }
    timeout "$DURATION_SECS" heaptrack --pid "$PID" --output "$OUTPUT/heaptrack" || [[ $? == 124 ]]
    ;;
  locks)
    command -v perf >/dev/null || { echo "perf is required" >&2; exit 1; }
    perf lock record -p "$PID" -o "$OUTPUT/locks.data" -- sleep "$DURATION_SECS"
    perf lock report -i "$OUTPUT/locks.data" > "$OUTPUT/locks.txt"
    ;;
  memory)
    printf 'unix_seconds,rss_kib,vsz_kib\n' > "$OUTPUT/memory.csv"
    for ((sample = 0; sample < DURATION_SECS; sample++)); do
      read -r rss vsz < <(ps -o rss=,vsz= -p "$PID")
      printf '%s,%s,%s\n' "$(date +%s)" "$rss" "$vsz" >> "$OUTPUT/memory.csv"
      sleep 1
    done
    ;;
esac