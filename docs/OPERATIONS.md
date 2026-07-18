# Operations guide

## Health endpoints

- `/health/live`: the process event loop is active;
- `/health/ready`: listeners and components were constructed and the daemon has not begun
  shutdown;
- `/v1/status`: current connection/subscription cardinality and counters;
- `/metrics`: Prometheus text exposition.

Current readiness does not actively query Kafka on every request. Alert on consumer
errors and lag separately. Task 008 adds a configurable Kafka-health readiness policy.

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

Task 008 adds histograms for decode, match, enqueue, wire write, and end-to-end timestamp
latency, plus consumer lag and rebalance counters.

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

Files in `deploy/kubernetes` provide:

- ConfigMap with non-secret local-style configuration;
- Secret placeholder;
- Deployment with probes, resources, security context, and topology spread;
- ClusterIP Service;
- PodDisruptionBudget; and
- optional HorizontalPodAutoscaler.

For full-stream mode, derive a unique Kafka group id from pod identity. A plain Deployment
with an environment-only group id would accidentally make pods share a group. The
example uses the pod name through the Downward API and documents a required config
rendering step; task 010 makes this production-grade with a startup templater or native
`group_id_suffix` setting.

### systemd

The unit runs an unprivileged user, restarts on failure, applies filesystem protections,
and sends SIGTERM. Install configuration under `/etc/kafka-edge-router/router.toml`.

### Container

The Dockerfile uses a Rust build stage and a slim non-root runtime. Generate and commit
The committed `Cargo.lock` is enforced with `--locked` for reproducible builds.

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
4. Remember current retries are volatile across restart.

### Kafka unavailable

1. Confirm broker DNS and TLS/SASL credentials.
2. Inspect librdkafka errors and consumer group state.
3. Decide whether readiness should be withdrawn manually through deployment controls.
4. Do not restart repeatedly if the outage is external and backoff is functioning.
