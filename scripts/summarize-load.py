#!/usr/bin/env python3
"""Summarize bounded router-load phase reports and fail on correctness gaps."""

from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

MAX_PHASE_FILES = 10_000
PROTOCOLS = ("websocket", "sse", "grpc", "webhook")


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: summarize-load.py ARTIFACT_DIRECTORY", file=sys.stderr)
        return 2
    root = Path(sys.argv[1])
    paths = sorted(root.glob("phase-*.json"))
    if not paths or len(paths) > MAX_PHASE_FILES:
        print(f"expected 1..={MAX_PHASE_FILES} phase reports", file=sys.stderr)
        return 2
    reports = [json.loads(path.read_text(encoding="utf-8")) for path in paths]
    summary: dict[str, object] = {
        "schema_version": 1,
        "phases": len(reports),
        "successful_publishes": sum(item["successful_publishes"] for item in reports),
        "failed_publishes": sum(item["failed_publishes"] for item in reports),
        "elapsed_ms": sum(item["elapsed_ms"] for item in reports),
    }
    correctness_ok = summary["failed_publishes"] == 0
    for protocol in PROTOCOLS:
        values = [item[protocol] for item in reports]
        expected = sum(item["expected"] for item in values)
        received = sum(item["received"] for item in values)
        percentiles: dict[str, object] = {}
        for name in ("p50_us", "p95_us", "p99_us", "p999_us", "max_us"):
            samples = [item[name] for item in values if item[name] is not None]
            percentiles[f"median_phase_{name}"] = (
                statistics.median(samples) if samples else None
            )
            percentiles[f"worst_phase_{name}"] = max(samples) if samples else None
        summary[protocol] = {
            "expected": expected,
            "received": received,
            "delivery_ratio": received / expected if expected else None,
            **percentiles,
        }
        correctness_ok = correctness_ok and received >= expected
    summary["correctness_ok"] = correctness_ok
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if correctness_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())