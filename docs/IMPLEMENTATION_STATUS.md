# Implementation status

Legend: **implemented**, **scaffolded**, **planned**, **not planned**.

| Capability | Status | Evidence / next task |
|---|---|---|
| Cargo workspace and crate boundaries | implemented | root `Cargo.toml` |
| Header-only Kafka decoding | implemented | `router-kafka/decoder.rs` |
| Exact/wildcard indexed matching | implemented | `router-core/route.rs` |
| Tenant-isolated subscriptions | implemented | core + adapter checks |
| Bounded per-connection queues | implemented | `router-core/router.rs` |
| Slow-consumer eviction | implemented | core tests |
| Kafka explicit commit loop | implemented | `router-kafka/ingestor.rs` |
| Idempotent Kafka publisher | implemented | `router-kafka/publisher.rs` |
| WebSocket dynamic subscriptions | implemented | `router-api/http.rs` |
| SSE fixed subscriptions | implemented | `router-api/http.rs` |
| gRPC fixed and bidirectional streams | implemented | `router-api/grpc.rs` |
| HTTP/gRPC publish | implemented | API adapters |
| Static webhook workers | implemented | `router-webhook` |
| Webhook volatile retries and HMAC | implemented | `router-webhook/manager.rs` |
| Health/readiness/status/metrics | implemented | `router-api` |
| SIGINT/SIGTERM graceful shutdown | implemented | `routerd/main.rs` |
| Local Kafka and deployment files | scaffolded | structurally validated; runtime validation in task 000/010 |
| Compile against pinned dependency APIs | scaffolded | task 000 |
| Committed `Cargo.lock` | planned | task 000 |
| Serialized route-index mutation and safe empty-bucket cleanup | implemented | `router-core/router.rs` |
| Route-index concurrency/property proof | planned | task 001 |
| Kafka rebalance/commit integration suite | planned | task 002 |
| WS limits and rate controls | planned | task 003 |
| SSE reconnect policy tests | planned | task 004 |
| gRPC limits/interceptors | planned | task 005 |
| Publish authorization/idempotency suite | planned | task 006 |
| Durable webhook retry and DLQ | planned | task 007 |
| OpenTelemetry and latency histograms | planned | task 008 |
| JWT/JWKS, TLS, DNS-aware SSRF | planned | task 009 |
| Load, soak, SBOM, signed release | planned | task 010 |
| Arbitrary payload expressions | not planned | violates hot-path design |
| Exactly-once live delivery | not planned in MVP | needs separate durable mode |
