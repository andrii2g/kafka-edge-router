# Observability

The router exposes Prometheus metrics at `/metrics`, structured logs through `tracing`, and
optional OTLP/HTTP traces. Metrics collection is always local and lock-free. OTLP construction,
export, and shutdown errors are logged and never stop routing.

## Configuration

`observability.opentelemetry.enabled` controls trace export. `endpoint` is the collector base
URL; the exporter appends `/v1/traces`. `sampling_ratio` is a fixed root sampling ratio in
`0..=1`. Export and shutdown each have explicit time limits.

`observability.kafka_readiness.enabled` makes HTTP and gRPC readiness depend on recent Kafka
health. A transition requires `success_threshold` consecutive healthy checks or
`failure_threshold` consecutive unhealthy checks. Disable it when deployments must remain ready
during broker maintenance.

## Cardinality and sensitive data

| Metric | Labels | Maximum series per process |
|---|---|---:|
| `router_operation_latency_seconds` | `stage` | 7 histograms |
| `router_active_connections` | `protocol` | 4 |
| `router_active_subscriptions` | `protocol` | 4 |
| publish counters | `protocol` | 2 each |
| rebalance counter | `event` | 3 |

Tenant ids, message ids, Kafka keys, topics, partitions, offsets, webhook ids, destination URLs,
and error strings are never metric labels. Trace spans may include message id, Kafka
topic/partition/offset, protocol, and retry attempt for sampled diagnostic correlation. Message
id and topic are high-cardinality attributes and must not be promoted into backend metric
dimensions.

Bearer tokens, signing secrets, Kafka credentials, arbitrary authorization headers, payloads,
and full webhook URLs are sensitive. They must not appear in metrics, spans, logs, dashboards,
or alert annotations. Trace context is accepted only from the bounded `traceparent` and
`tracestate` Kafka headers.

## Dashboard and response

Import `deploy/observability/grafana-dashboard.json`; load
`deploy/observability/prometheus-alerts.yaml` into Prometheus.

| Dashboard panel or alert | Operator action |
|---|---|
| Kafka lag / `KafkaRouterLagHigh` | Check broker reachability, assignments, consumer errors, and downstream queue pressure. Scale only after determining whether fan-out or Kafka is limiting. |
| Commit errors / `KafkaRouterCommitErrors` | Check broker availability and ACLs. Expect duplicate delivery after restart until commits recover. |
| Queue-full rate / `KafkaRouterQueuePressure` | Identify the affected protocol from connection gauges and slow-consumer logs. Reduce fan-out or disconnect slow clients. |
| Webhook failures / `KafkaRouterWebhookFailures` | Check destination health without logging URLs or secrets; inspect retry and DLQ counters. |
| Assigned partitions / `KafkaRouterNoAssignment` | Check group membership, topic existence, and rebalance errors. Readiness may be false when Kafka dependency is enabled. |
| Latency p95 | Compare stages. Decode/match suggests local CPU pressure; protocol-write or webhook-attempt suggests downstream I/O. |

## Trace lifecycle

Kafka `traceparent` and optional `tracestate` headers become the remote parent of
`message.route`. Protocol writes, durable persistence, and webhook attempts are descendants.
The tracer provider flushes after listeners and workers drain, bounded by
`shutdown_timeout_ms`.

## Overhead benchmark

Run:

```bash
cargo bench --locked -p router-core --bench observability
```

The `baseline`, atomic `counter`, and fixed-bucket `histogram` cases isolate local observation
cost. Record the CPU, OS, Rust version, and Criterion estimates in `CHANGELOG.md` when changing
the metrics implementation. Generated Criterion output remains untracked.
