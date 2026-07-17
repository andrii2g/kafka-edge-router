# Task 008: Add production observability

## Goal

Provide actionable metrics, traces, logs, and dashboard assets without exposing secrets or
creating uncontrolled high-cardinality telemetry.

## Required work

1. Replace or wrap manual metrics with a tested metrics abstraction supporting counters,
   gauges, and histograms.
2. Add decode, match, enqueue, protocol-write, webhook-attempt, publish, and end-to-end
   latency histograms.
3. Add Kafka lag, rebalance, assignment, and commit-error metrics.
4. Add active connection/subscription gauges by protocol without tenant labels by default.
5. Propagate message id and trace context from Kafka headers where present.
6. Add OpenTelemetry export configuration and graceful flush.
7. Define which attributes are high cardinality or sensitive.
8. Add Grafana dashboard and Prometheus alert-rule examples.
9. Test metric names, label bounds, and trace shutdown.
10. Add an optional readiness dependency on Kafka health with hysteresis.

## Acceptance criteria

- no token, secret, payload, or arbitrary destination URL appears in telemetry;
- cardinality bounds are documented;
- dashboard panels map to runbook actions;
- telemetry failure cannot crash routing; and
- benchmarks quantify observability overhead.

## Commit title

```text
feat(obs): add bounded metrics traces and dashboards
```
