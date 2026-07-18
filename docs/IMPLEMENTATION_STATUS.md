# Implementation status

Legend: **implemented**, **scaffolded**, **planned**, **not planned**.

Here, **implemented** means a non-placeholder runtime path exists and is wired into the
daemon. It does not imply that later production-hardening, integration, load, or failure
injection tasks are complete. The evidence column names those remaining proof gaps.

| Capability | Status | Evidence / remaining proof |
|---|---|---|
| Cargo workspace and crate boundaries | implemented | root `Cargo.toml`; `router-core` remains transport-independent and Kafka/API/webhook dependencies stay in their owning crates |
| Header-only Kafka decoding | implemented | `router-kafka/decoder.rs` reads routing metadata from headers and copies payload bytes without parsing them; decoder contract tests remain task 002 |
| Exact/wildcard indexed matching | implemented | `router-core/route.rs`; exhaustive 64-shape candidate uniqueness and indexed/reference equivalence tests |
| Tenant-isolated subscriptions | implemented | core rejects tenant mismatches; HTTP/gRPC adapters authorize then rewrite filters to the authenticated principal; adapter security suites remain tasks 003-006 and 009 |
| Bounded per-connection queues | implemented | Tokio bounded MPSC registration in `router-core/router.rs`; zero/exact/over-limit tests cover core, live API, and webhook configuration |
| Slow-consumer eviction | implemented | non-blocking `try_send`, saturating strike count, zero/one/configured-strike tests, and route cleanup assertions in core |
| Kafka explicit commit loop | implemented | `router-kafka/ingestor.rs` disables auto commit/store and requests async commit after dispatch policy; broker, rebalance, and commit integration proof remains task 002 |
| Kafka producer with idempotence enforced | implemented | `router-kafka/publisher.rs` reapplies `enable.idempotence=true` and `acks=all` after free-form properties; broker/header/idempotency tests remain tasks 002 and 006 |
| WebSocket dynamic subscriptions | implemented | `router-api/http.rs` wires authenticated connect, subscribe, unsubscribe, ping, bounded delivery, and RAII cleanup; limits/compatibility tests remain task 003 |
| SSE fixed subscriptions | implemented | `router-api/http.rs` wires authenticated fixed filters, bounded delivery, event ids, keep-alives, and RAII cleanup; reconnect/framing tests remain task 004 |
| gRPC fixed and bidirectional streams | implemented | `router-api/grpc.rs` and `router-proto` wire fixed subscribe, bidi commands, raw-byte delivery, and RAII cleanup; interceptor/contract tests remain task 005 |
| HTTP/gRPC publish | implemented | both adapters authorize tenant, call `MessagePublisher`, and return Kafka coordinates; authorization and idempotency suites remain task 006 |
| Static webhook workers | implemented | `WebhookManager` creates one bounded core registration and ordered long-lived worker per configured destination; recovery/durability remains task 007 |
| Webhook volatile retries and HMAC | implemented | bounded attempts/backoff, timeout, no redirects, idempotency headers, and HMAC-SHA256 in `router-webhook/manager.rs`; unit tests cover signing/status classification, end-to-end retry recovery remains task 007 |
| Health/readiness/status/metrics | implemented | HTTP health/status/Prometheus handlers, gRPC status, atomic counters, and startup/shutdown gates are wired; telemetry histograms and endpoint integration tests remain task 008 |
| Graceful SIGINT/SIGTERM shutdown on Unix | implemented | `routerd/main.rs` lowers readiness, broadcasts shutdown, drains to a deadline, then aborts; Windows supports Ctrl-C, and lifecycle/signal integration tests are still absent |
| Local Kafka and deployment files | scaffolded | repository validation passed; Docker/runtime checks await an installed Docker CLI and task 010 |
| Compile against pinned dependency APIs | implemented | verified with Rust 1.88 locked check, Clippy, tests, both config checks, and release build; CI also runs locked gates |
| Committed `Cargo.lock` | implemented | root `Cargo.lock`; CI, Docker, Make, and release builds use `--locked` |
| Serialized route-index mutation and safe empty-bucket cleanup | implemented | one mutation mutex covers connection/index mutation and conditional cleanup; churn/repopulation races assert zero leaked route keys |
| Route-index concurrency/property proof | implemented | exhaustive/randomized matcher properties, barrier races for subscribe/unregister and unsubscribe/dispatch, atomic duplicate tests, repeated guard cleanup, and the `matcher` benchmark |
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
