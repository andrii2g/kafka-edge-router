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
| Tenant-isolated subscriptions | implemented | core rejects tenant mismatches; HTTP/gRPC adapters authorize then rewrite filters to the authenticated principal; generated-client tests cover cross-tenant subscription and publish denial, while authentication hardening remains task 009 |
| Bounded per-connection queues | implemented | Tokio bounded MPSC registration in `router-core/router.rs`; zero/exact/over-limit tests cover core, live API, and webhook configuration |
| Slow-consumer eviction | implemented | non-blocking `try_send`, saturating strike count, zero/one/configured-strike tests, and route cleanup assertions in core |
| Kafka explicit commit loop | implemented | `router-kafka/ingestor.rs` disables auto commit/store and requests async commit after dispatch policy; broker tests cover commits, restart redelivery, poison policy, and rebalances |
| Kafka producer with idempotence enforced | implemented | unit tests prove free-form properties cannot override `enable.idempotence=true`, `acks=all`, or the delivery timeout; broker tests prove acknowledgement coordinates and keyed ordering |
| WebSocket dynamic subscriptions | implemented | authenticated upgrade, stable command/error contract, queue/frame/message/rate limits, intentional close codes, RAII cleanup, cancellation, reconnect, and tenant-negative adapter tests |
| SSE fixed subscriptions | implemented | authenticated fixed filters, strict query and queue validation, event id/name/JSON framing, virtual-time keep-alives, explicit live-only reconnect behavior, anti-buffering headers, tenant isolation, slow-consumer eviction, and cancellation cleanup are covered |
| gRPC fixed and bidirectional streams | implemented | generated-client integration tests cover fixed delivery/cancellation and bidi subscribe, unsubscribe, ping, delivery, invalid commands, duplicate ids, tenant mismatch, publisher absence, and stable statuses; an unpolled-stream test proves bounded slow-consumer eviction and RAII cleanup |
| HTTP/gRPC publish | implemented | both adapters share payload/metadata validation, generated or caller-stable ids, tenant-prefixed explicit keys, separate publish permission, size caps, audit-safe counters, and stable timeout/queue/backend mappings; tests prove JSON/base64/raw-byte fidelity |
| Static webhook workers | implemented | volatile mode uses bounded core registrations; durable mode uses compiled matching, pre-commit commands, destination-fenced consumers, and bounded recovery |
| Webhook retry, HMAC, and DLQ | implemented | bounded volatile or persisted retry, no redirects, stable idempotency/HMAC, secret-free records, terminal DLQ, metrics, and real-Kafka restart recovery |
| Health/readiness/status/metrics | implemented | HTTP health/status/Prometheus handlers, authenticated gRPC status, standard gRPC health tied to readiness, atomic counters, and startup/shutdown gates are wired; telemetry histograms remain task 008 |
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
| gRPC limits/shared authentication | implemented | configurable inbound/outbound message caps, per-connection concurrency, load shedding, keepalive, health, optional reflection, shared authentication enforcement, and generated-client contract tests |
| Publish authorization/idempotency suite | implemented | HTTP/gRPC adapter tests cover tenant and permission denial, generated/reused ids, exact JSON/base64/raw bytes, validation before backend calls, stable errors, and payload-free metrics |
| Durable webhook retry and DLQ | implemented | broker-backed delivery/retry/DLQ topics, source pre-commit barrier, bounded recovery, explicit duplicate windows, and idempotent receiver restart test |
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
| 004: SSE productionization | implemented | seven deterministic adapter tests cover id/name/data framing, escaped multiline JSON, virtual-time keep-alives, strict query limits, live-only reconnect, anti-buffering/no-compression headers, tenant isolation, slow consumers, and cancellation cleanup |
| 005: gRPC productionization | implemented | generated clients prove fixed and bidirectional stream contracts, stable auth/input/publisher statuses, cancellation cleanup, bounded slow receivers, readiness-backed standard health, optional reflection, explicit transport flow control, and unchanged protobuf field numbers |
| 006: Publish API hardening | implemented | shared HTTP/gRPC validation, separate publish authorization, bounded JSON/base64/raw payloads, stable generated/reused ids, tenant-safe explicit ordering keys, classified Kafka failures, producer-invariant tests, broker acknowledgements, and audit-safe counters satisfy the acceptance criteria |
| 007: Durable webhooks | implemented in current working tree | ADR 0005, explicit mode split, pre-commit durable fan-out, persisted retry/DLQ state, ownership groups, audit-safe metrics, runbook, and real-Kafka restart recovery satisfy the acceptance criteria |
