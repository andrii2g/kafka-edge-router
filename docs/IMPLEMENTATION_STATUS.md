# Implementation status

Legend: **implemented**, **scaffolded**, **planned**, **not planned**.

Here, **implemented** means a non-placeholder runtime path exists and is wired into the
daemon. It does not imply that later production-hardening, integration, load, or failure
injection tasks are complete. The evidence column names those remaining proof gaps.

| Capability | Status | Evidence / remaining proof |
|---|---|---|
| Cargo workspace and crate boundaries | implemented | root `Cargo.toml`; `router-core` remains transport-independent and Kafka/API/webhook dependencies stay in their owning crates |
| Header-only Kafka decoding | implemented | `router-kafka/decoder.rs` reads routing metadata from headers and copies payload bytes without parsing them; unit tests cover missing/duplicate/invalid headers, pairing, limits, and defaults |
| Exact/wildcard indexed matching | implemented | `router-core/route.rs`; exhaustive 64-shape candidate uniqueness and indexed/reference equivalence tests |
| Tenant-isolated subscriptions | implemented | core rejects tenant mismatches; HTTP/gRPC adapters authorize then rewrite filters to the authenticated principal; gRPC/publish adapter suites remain tasks 005-006 and authentication hardening remains task 009 |
| Bounded per-connection queues | implemented | Tokio bounded MPSC registration in `router-core/router.rs`; zero/exact/over-limit tests cover core, live API, and webhook configuration |
| Slow-consumer eviction | implemented | non-blocking `try_send`, saturating strike count, zero/one/configured-strike tests, and route cleanup assertions in core |
| Kafka explicit commit loop | implemented | `router-kafka/ingestor.rs` disables auto commit/store and requests async commit after dispatch policy; broker tests cover commits, restart redelivery, poison policy, and rebalances |
| Kafka producer with idempotence enforced | implemented | `router-kafka/publisher.rs` reapplies `enable.idempotence=true` and `acks=all` after free-form properties; broker key/ordering tests are complete; publish authorization/idempotency remains task 006 |
| WebSocket dynamic subscriptions | implemented | authenticated upgrade, stable command/error contract, queue/frame/message/rate limits, intentional close codes, RAII cleanup, cancellation, reconnect, and tenant-negative adapter tests |
| SSE fixed subscriptions | implemented | authenticated fixed filters, strict query and queue validation, event id/name/JSON framing, virtual-time keep-alives, explicit live-only reconnect behavior, anti-buffering headers, tenant isolation, slow-consumer eviction, and cancellation cleanup are covered |
| gRPC fixed and bidirectional streams | implemented | `router-api/grpc.rs` and `router-proto` wire fixed subscribe, bidi commands, raw-byte delivery, and RAII cleanup; interceptor/contract tests remain task 005 |
| HTTP/gRPC publish | implemented | both adapters authorize tenant, call `MessagePublisher`, and return Kafka coordinates; authorization and idempotency suites remain task 006 |
| Static webhook workers | implemented | `WebhookManager` creates one bounded core registration and ordered long-lived worker per configured destination; recovery/durability remains task 007 |
| Webhook volatile retries and HMAC | implemented | bounded attempts/backoff, timeout, no redirects, idempotency headers, and HMAC-SHA256 in `router-webhook/manager.rs`; unit tests cover signing/status classification, end-to-end retry recovery remains task 007 |
| Health/readiness/status/metrics | implemented | HTTP health/status/Prometheus handlers, gRPC status, atomic counters, and startup/shutdown gates are wired; telemetry histograms and endpoint integration tests remain task 008 |
| Graceful SIGINT/SIGTERM shutdown on Unix | implemented | `routerd/main.rs` lowers readiness, broadcasts shutdown, drains to a deadline, then aborts; Windows supports Ctrl-C, and lifecycle/signal integration tests are still absent |
| Local Kafka and container workflow | implemented | pinned Apache Kafka 4.3.1 starts through Compose; the broker-backed integration suite, locked image build, both config checks, and HTTP publish smoke path pass |
| Production deployment files | scaffolded | Kubernetes, systemd, and container guidance exists; load, soak, release, and production deployment validation remain task 010 |
| Compile against pinned dependency APIs | implemented | verified with Rust 1.88 locked check, Clippy, tests, both config checks, and release build; CI also runs locked gates |
| Committed `Cargo.lock` | implemented | root `Cargo.lock`; CI, Docker, Make, and release builds use `--locked` |
| Serialized route-index mutation and safe empty-bucket cleanup | implemented | one mutation mutex covers connection/index mutation and conditional cleanup; churn/repopulation races assert zero leaked route keys |
| Route-index concurrency/property proof | implemented | exhaustive/randomized matcher properties, barrier races for subscribe/unregister and unsubscribe/dispatch, atomic duplicate tests, repeated guard cleanup, and the `matcher` benchmark |
| Kafka rebalance/commit integration suite | implemented | isolated official-image harness covers header/default contracts, keyed ordering, valid/invalid commits, restart duplicates, and forced rebalances; CI runs it separately |
| WS limits and rate controls | implemented | bounded upgrade configuration, per-connection fixed-window limiter, subscription-cap errors, oversized-input `1009`, and slow-consumer `1013` coverage |
| SSE reconnect policy tests | implemented | `Last-Event-ID` is parsed and explicitly ignored with `X-SSE-Replay: unsupported`; tests prove reconnect receives only new live events |
| gRPC limits/interceptors | planned | task 005 |
| Publish authorization/idempotency suite | planned | task 006 |
| Durable webhook retry and DLQ | planned | task 007 |
| OpenTelemetry and latency histograms | planned | task 008 |
| JWT/JWKS, TLS, DNS-aware SSRF | planned | task 009 |
| Load, soak, SBOM, signed release | planned | task 010 |
| Arbitrary payload expressions | not planned | violates hot-path design |
| Exactly-once live delivery | not planned in MVP | needs separate durable mode |

## Completed task audit

| Task | Status | Acceptance evidence |
|---|---|---|
| 000: Bootstrap and compile | implemented | Rust 1.88 locked workspace gates, vendored protobuf generation, both config checks, release/container build, local Kafka HTTP smoke, and concurrent WebSocket/SSE same-message-id delivery pass; image `sha256:258a4a004ae9f5e85901622e9e8898aea790405e1b049de61e7ced6e13549e57` runs as UID/GID `10001:10001` |
| 001: Routing core hardening | implemented | exhaustive/reference and randomized matcher tests, barrier-coordinated lifecycle races, queue-cap hierarchy tests, zero/one strike semantics, route cleanup proofs, documented ThreadSanitizer path, and Criterion baseline are present |
| 002: Kafka integration | implemented | required broker mode against Apache Kafka 4.3.1 passes four isolated ordering, commit/poison, restart-duplicate, and forced-rebalance tests; decoder unit tests cover malformed header and payload boundaries |
| 003: WebSocket productionization | implemented | adapter suite covers command contract, stable errors, tenant isolation, frame/message/queue/subscription/rate limits, slow-consumer close, cancellation cleanup, and reconnect; compression remains explicitly disabled |
| 004: SSE productionization | implemented in current working tree | seven deterministic adapter tests cover id/name/data framing, escaped multiline JSON, virtual-time keep-alives, strict query limits, live-only reconnect, anti-buffering/no-compression headers, tenant isolation, slow consumers, and cancellation cleanup |
