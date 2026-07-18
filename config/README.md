# Configuration

`routerd` loads one TOML file and overlays environment variables whose names follow
`ROUTER__SECTION__FIELD`. `RUST_LOG` overrides `logging.filter`.

Examples:

```bash
export ROUTER__SERVER__HTTP_ADDR=127.0.0.1:8080
export ROUTER__KAFKA__CONSUMER__BROKERS=kafka.internal:9092
export RUST_LOG=routerd=debug,router_core=trace
cargo run -p routerd -- --config config/router.toml
```

Arrays and maps are clearer and less error-prone in TOML, so keep topics, Kafka
properties, bearer-token mappings, and webhook destinations in a mounted file.
Run `routerd --check-config` before deploying a change.

## WebSocket limits

`api.ws_max_message_bytes` caps a complete inbound command after frame reassembly, and
`api.ws_max_frame_bytes` caps each frame. The frame cap must not exceed the message cap.
`api.ws_max_commands_per_second` is a per-connection fixed-window application-command
budget. All values must be positive. Queue requests remain independently capped by
`api.max_stream_queue_capacity` and `router.max_queue_capacity`.

## gRPC limits and discovery

`api.grpc_max_decoding_message_bytes` and
`api.grpc_max_encoding_message_bytes` bound individual messages.
`api.grpc_concurrency_limit` limits concurrent requests per HTTP/2 connection; excess
requests are load-shed with `RESOURCE_EXHAUSTED`.
`api.grpc_keep_alive_interval_secs` and `api.grpc_keep_alive_timeout_secs` configure
HTTP/2 keepalive pings and acknowledgement deadlines. These numeric settings must be
positive.

`api.grpc_health_enabled` exposes the standard gRPC health service and tracks daemon
readiness. `api.grpc_reflection_enabled` exposes gRPC reflection v1. Reflection is useful
in the local configuration but should remain `false` in production.

## Publish policy

`api.publish_max_payload_bytes` is the shared raw-payload cap for HTTP and gRPC. It is
checked before Kafka send and must be positive. Keep it at or below the Kafka and memory
budget; the HTTP request-body limit independently caps the complete JSON request.

`auth.publish_tenants` is an explicit tenant allowlist applied after authentication and
tenant matching. Subscription credentials do not imply publish permission. In
`static_bearer` mode every listed publish tenant must also appear as a bearer-token
mapping.
## Webhook delivery mode

`webhooks.mode` is either `volatile` or `durable`; modes are never mixed. Volatile
mode uses each destination's bounded queue and loses pending retries on restart. Durable
mode requires at least one destination and distinct delivery, retry, and dead-letter
topics with equal partition counts.

`webhooks.durable.max_record_bytes` bounds serialized command allocation.
`max_recovery_records` bounds retry state materialized during one startup pass.
`delivery_timeout_ms` bounds Kafka acknowledgement waits. Durable Kafka properties may
set authentication/compression, but cannot override manual commits, idempotent production,
all-replica acknowledgements, earliest recovery, or the range assignment strategy.

See [the durable webhook runbook](../docs/WEBHOOK_OPERATIONS.md) before provisioning,
changing topic partitions/retention, or replaying dead letters.
