# Operations guide

## Health endpoints

- `/health/live`: the process event loop is active;
- `/health/ready`: listeners and components were constructed and the daemon has not begun
  shutdown;
- `/v1/status`: current connection/subscription cardinality and counters;
- `/metrics`: Prometheus text exposition;
- `grpc.health.v1.Health`: standard gRPC readiness for `router.v1.KafkaRouter` when
  `api.grpc_health_enabled` is true.

When `observability.kafka_readiness.enabled` is true, readiness follows recent Kafka health
through configured consecutive success and failure thresholds. Metrics and alerts remain
necessary because readiness is intentionally a coarse traffic gate.

## Startup checklist

1. Validate the configuration with `routerd --check-config`.
2. Confirm the Kafka topics and partition count.
3. Confirm each router node has a unique consumer group in full-stream mode.
4. Confirm production authentication is not `disabled`.
5. Confirm public TLS and network policy.
6. Confirm webhook allowlists and egress policy.
7. Confirm memory limits account for maximum connections times queue capacity and
   maximum message size.
8. Confirm termination grace exceeds `server.shutdown_grace_secs`.

## Capacity model

A conservative upper bound for queued payload references is:

```text
connections × queue_capacity × average retained message footprint
```

`Bytes` prevents a payload copy per local connection for the same message, but queued
messages from different Kafka records remain retained. Envelope objects and subscription
ids add overhead. `router.max_queue_capacity` is the absolute per-queue cap;
`api.max_stream_queue_capacity` is the lower public-request cap. Keep both below the
memory budget rather than treating a larger queue as a throughput optimization.

Webhook destinations have independent queues and can retain messages during retries.

## Key metrics

Current counters:

```text
router_kafka_messages_total
router_kafka_bytes_total
router_kafka_commit_errors_total
router_kafka_rebalances_total{event=\"assignment|revocation|error\"}
router_messages_valid_total
router_messages_invalid_total
router_messages_unmatched_total
router_matched_subscriptions_total
router_delivered_connections_total
router_full_connections_total
router_closed_connections_total
router_slow_consumer_disconnects_total
router_connections
router_subscriptions
router_protocol_connections_opened_total{protocol=...}
router_webhook_attempts_total
router_webhook_successes_total
router_webhook_failures_total
```

Latency histograms cover decode, match, enqueue, protocol write, webhook attempt, publish,
and end-to-end processing. Gauges expose aggregate Kafka lag, assigned partitions, and active
connections/subscriptions by bounded protocol label. See `docs/OBSERVABILITY.md` for the
complete label policy, dashboard mapping, and response actions.

## Suggested alerts

- readiness unavailable for more than the deployment rollout window;
- invalid messages greater than zero over a sustained interval;
- consumer lag increasing continuously;
- Kafka receive or commit errors;
- slow-consumer disconnect rate above baseline;
- webhook terminal failures greater than zero;
- webhook retry attempts increasing without successes;
- process resident memory near limit; and
- unexpected restart count.

Alert thresholds require workload baselines; do not copy arbitrary static values.

## Deployments

### Kubernetes

`deploy/kubernetes/base` provides a production-oriented Deployment, TLS proxy, Service,
PodDisruptionBudget, HorizontalPodAutoscaler, NetworkPolicy, resources, probes, and topology
constraints. `deploy/kubernetes/overlays/rc` selects the release-candidate namespace and
image. Render and server-side dry-run the overlay before applying it.

The Deployment injects immutable `POD_UID`; `kafka.group_id_suffix_env = "POD_UID"` makes
startup resolve a unique full-stream consumer group for every replica. Configuration, JWKS,
and TLS material are separate externally managed Secrets and are never committed. See
`deploy/kubernetes/README.md` and `docs/RELEASE.md` for rollout and rollback gates.

### SSE reverse proxies

For `/v1/events`, disable response buffering and choose every proxy, ingress, and load
balancer idle timeout to exceed `api.sse_keep_alive_secs`. The application emits
`Cache-Control: no-cache, no-transform` and `X-Accel-Buffering: no`, but operators must
still verify their complete proxy chain:

- NGINX Ingress: set `nginx.ingress.kubernetes.io/proxy-buffering: "off"` and set
  `nginx.ingress.kubernetes.io/proxy-read-timeout` to a number of seconds comfortably
  above the keep-alive interval.
- Traefik: streaming responses are flushed immediately when recognized; if a middleware
  or deployment prevents recognition, configure the service
  `responseForwarding.flushInterval` to a negative duration for immediate flushing.
- Envoy-based ingress: disable the route stream idle timeout or set it above the
  keep-alive interval, and confirm no response buffer filter applies to the SSE route.

See the official [Ingress-NGINX annotations](https://kubernetes.github.io/ingress-nginx/user-guide/nginx-configuration/annotations/),
[Traefik response forwarding](https://doc.traefik.io/traefik/routing/services/#response-forwarding),
and [Envoy route timeout](https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/route/v3/route_components.proto)
documentation. Test through the external ingress, not only against the pod IP: an SSE
event and periodic keep-alive comment must arrive without accumulating into batches.

### systemd

The unit runs an unprivileged user, restarts on failure, applies filesystem protections,
and sends SIGTERM. Install configuration under `/etc/kafka-edge-router/router.toml`.

### Container

The Dockerfile uses a Rust build stage and a slim non-root runtime. The committed
`Cargo.lock` is enforced with `--locked` for reproducible builds.

## Rolling upgrades

- Preserve protobuf field numbers and WebSocket/SSE envelope compatibility.
- Add optional fields before making clients depend on them.
- Deploy enough capacity to absorb reconnect storms.
- Set readiness false before termination and allow streams to drain.
- Expect Kafka records to be duplicated around crashes or commit races.
- Use stable message ids during replay.

## Incident procedures

### Invalid Kafka records

1. Check `router_messages_invalid_total` and logs with topic/partition/offset.
2. Inspect headers without exposing payload or secrets.
3. Correct the producer.
4. Republish with the original message id when semantic identity is unchanged.
5. Consider a quarantine topic before changing `commit_invalid_messages` to false.

### Slow-consumer spike

1. Break down opened connections by protocol and inspect full/disconnect counters.
2. Check network latency and client release changes.
3. Confirm queue capacities were not reduced unexpectedly.
4. Avoid simply increasing queues; estimate memory and determine whether clients need a
   coalescing or durable mode.

### Webhook failures

1. Check status distribution in logs and attempt/success/failure counters.
2. Confirm DNS, certificates, egress policy, and destination rate limits.
3. Verify the receiver honors idempotency keys.
4. Identify the configured delivery mode; volatile retries can be lost on restart, while durable mode recovery depends on its Kafka topics.

### Kafka unavailable

1. Confirm broker DNS and TLS/SASL credentials.
2. Inspect librdkafka errors and consumer group state.
3. Decide whether readiness should be withdrawn manually through deployment controls.
4. Do not restart repeatedly if the outage is external and backoff is functioning.
