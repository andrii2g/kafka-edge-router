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

## Observability

`observability.opentelemetry` configures optional OTLP/HTTP trace export. The sampling ratio must
be within `0..=1`; exporter and shutdown timeouts must be positive. Exporter initialization and
delivery failures do not stop routing.

`observability.kafka_readiness` optionally gates HTTP and gRPC readiness on recent broker or
consumer activity. Positive consecutive success and failure thresholds provide hysteresis.
Keep this disabled when readiness must remain independent from planned Kafka maintenance. See
[the observability runbook](../docs/OBSERVABILITY.md) for attribute and cardinality policy.

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

## Production identity and transport

Production selects server.security_mode = protected_proxy. Both daemon listeners must be
loopback addresses and authentication cannot be disabled. A colocated TLS proxy exposes
the public ports, strips client-supplied identity headers, and forwards only authenticated
traffic. Use proxy_mtls when the proxy verifies client certificates and maps the injected
identity through auth.proxy_identities.

JWT mode requires auth.jwt.jwks_path, issuer, audience, an asymmetric algorithms list,
tenant and scope claim names, explicit subscribe/publish scope names, refresh interval,
and JWKS byte/key caps. The JWKS file is loaded before listeners start and refreshed
without restart; an invalid refresh retains the last valid cache.

## Process and principal limits

router.max_connections and router.max_subscriptions cap process cardinality. Their
per-tenant counterparts prevent one authenticated principal from exhausting the process
budget. api.global_commands_per_second and api.global_publishes_per_second cap aggregate
rates; the principal variants cap each tenant. api.max_rate_limit_principals bounds the
fixed-window counter table. Limit rejection is exposed through
router_security_limit_rejections_total.

## Webhook network policy

Each destination can restrict allowed_hosts and allowed_ports. An empty port list permits
only the scheme default, normally HTTPS 443. DNS is resolved and every address is checked
before each attempt; validated addresses are pinned to that connection. Environment HTTP
proxies and redirects are disabled so resolution cannot bypass the private-address
policy.