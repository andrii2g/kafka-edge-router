# Changelog

All notable changes will be documented here. The project follows Semantic Versioning.



## Unreleased
- Final task audit confirmed Tasks 000-009 against their acceptance criteria and retained
  Task 010 as scaffolded until its release-candidate qualification gates are executed.
- Added durable-webhook restart and crash-boundary tests to the broker-backed CI command;
  all four Kafka integration tests and both durable recovery tests pass against Kafka 4.3.1.
- Restored standalone validation of the generic production configuration while keeping
  mandatory `POD_UID` group suffixing in the Kubernetes-specific configuration.
- Added bounded WS/SSE/gRPC/webhook load generation, expanded matcher/dispatch benchmarks,
  multi-hour soak orchestration, profile helpers, and machine-readable result summaries.
- Added production Kustomize base and RC overlay with Envoy TLS termination, pod-unique Kafka
  group identity, external Secrets, resource/disruption/topology controls, and network policy.
- Added a locked tagged release workflow for multi-architecture images and binaries, SBOM,
  provenance, vulnerability scans, checksums, keyless signatures, curated RC notes, and rollback.
- Task 010 local baseline measured 200 successful publishes with exact 400/400 WS, SSE, and
  gRPC deliveries at a controlled 20 publishes/second; this is a functional sample, not a
  capacity or soak claim. The multi-hour K3s soak and game day remain release gates.
- Built final local image kafka-edge-router:task010-final as
  sha256:d7be144b965022375c8fcfcd4fe1c2c43e48de109e8af0303759caa0a1c0e790;
  it runs as 10001:10001, passes its configuration check, and has zero fixed HIGH or
  CRITICAL findings under the release Trivy policy.

- Added reloadable asymmetric JWT/JWKS validation, explicit subscribe/publish scopes, proxy-mTLS identity mapping, and loopback-only protected proxy mode.
- Hardened webhook egress with per-attempt DNS validation and pinning, special-address rejection, direct-only proxy policy, redirect blocking, and destination port allowlists.
- Added global/per-tenant connection and subscription caps, bounded global/per-principal command and publish rates, rejection metrics, fuzz harnesses, secret scanning, and container scanning.

- Added fixed-bucket latency histograms for decode, match, enqueue, protocol write, webhook
  attempts, publish, and end-to-end handling.
- Added bounded protocol gauges, Kafka lag/assignment metrics, W3C trace propagation, optional
  nonfatal OTLP/HTTP export with graceful flush, and hysteretic Kafka readiness.
- Added a Grafana dashboard, Prometheus alert examples, sensitive/high-cardinality attribute
  policy, and an operator response map.

- Added explicit volatile and Kafka-backed durable webhook delivery modes.
- Durable webhook commands are acknowledged before source offset commit and retain the
  original message id across restart-safe retries.
- Added bounded retry recovery, terminal dead-letter publication, destination ownership
  groups, secret-free versioned records, and dedicated durability metrics.
- Added ADR 0005 and an operator runbook for topic retention and DLQ replay.

### Added

- Added WebSocket adapter coverage for commands, tenant isolation, queue and subscription
  limits, rate limiting, oversized input, slow-consumer eviction, cancellation, and reconnect.
- Added deterministic SSE adapter coverage for framing, escaped multiline JSON, virtual-time
  keep-alives, strict query limits, tenant isolation, slow-consumer eviction, and cleanup.
- Added generated-client gRPC integration coverage for fixed and bidirectional streams,
  authentication and tenant denial, invalid commands, duplicate subscriptions, cancellation,
  bounded slow receivers, publisher availability, standard health, and reflection.
- Added HTTP/gRPC publish contract tests for authorization, idempotency identity, exact
  JSON/base64/raw bytes, validation, ordering keys, backend errors, and safe metrics.

### Changed

- Added bounded WebSocket frame/message configuration, per-connection command budgets,
  stable application error codes, and explicit `1009`/`1013` close reasons.
- Kept WebSocket per-message compression disabled pending CPU and retained-memory benchmarks.
- Made SSE reconnect explicitly live-only: `Last-Event-ID` is parsed but ignored, responses
  advertise replay as unsupported, and proxy buffering/idle-timeout guidance is documented.
- Added configurable gRPC message-size, per-connection concurrency, load-shedding, and
  HTTP/2 keepalive limits, centralized RPC authentication, readiness-backed standard health,
  and locally enabled but production-disabled reflection.
- Defined a shared bounded publish contract with an explicit tenant allowlist, generated or
  caller-stable message ids, JSON/base64 HTTP modes, additive gRPC ordering keys,
  tenant-prefixed Kafka keys, classified timeout/queue-full errors, and publish counters.

### Validation

- Task 008 observability benchmark ran in WSL Docker on Rust 1.88.0, Linux x86_64, hosted on
  an AMD Ryzen 7 8845HS. Estimates: baseline 870.88 ps, atomic counter 3.3594 ns, and
  fixed-bucket histogram 5.3202 ns per observation.

- Re-audited tasks 000-004 against their acceptance criteria with Docker available.
- Built `kafka-edge-router:audit-000-004` from the locked workspace as
  `sha256:258a4a004ae9f5e85901622e9e8898aea790405e1b049de61e7ced6e13549e57`;
  the runtime image uses UID/GID `10001:10001`.
- Passed both configuration checks, the local Kafka HTTP publish smoke path, and a
  concurrent WebSocket/SSE probe that received the same stable message id using the built
  container.
- Passed all four required broker-backed Kafka integration tests against Apache Kafka
  4.3.1: ordering, commit/poison policy, restart duplicate, and forced rebalance.

## 18.07.2026

### Added

- Committed `Cargo.lock` for the Rust 1.88 workspace baseline.
- Added exhaustive and deterministic randomized route-matcher properties, barrier-based subscription lifecycle race tests, queue-cap hierarchy coverage, and Criterion routing benchmarks.
- Added isolated Kafka contract and broker integration suites for headers, keyed ordering, commits, restart duplicates, and forced rebalances.

### Changed

- Updated GitHub Actions to Node 24-compatible checkout and replaced the Node 20
  RustSec action with a locked `cargo-audit 0.22.2` installation.
- Installed libcurl development headers in Linux CI, release, and container builds for
  the vendored librdkafka build.
- Adapted route candidate inference, Kafka producer delivery receipts, protobuf generation,
  gRPC stream error propagation, and warning-level API drift to the locked dependency APIs.
- Enforced `--locked` in CI, release, Docker, and standard Make build commands.
- Reject duplicate routing headers, stop consumption at uncommitted malformed records, and expose Kafka commit-error and rebalance counters.
- Documented route-mutation linearization, bounded in-flight unsubscribe behavior,
  ThreadSanitizer execution, Loom/Miri rationale, and remaining mutation-lock contention.
- Unified HTTP and gRPC live-stream queue-cap boundary validation while retaining the
  independent core cap for live streams and static webhook workers.
- Made repository validation ignore generated build output and validate shell scripts
  correctly on Windows; shell scripts are now tracked as executable.

### Validation

- Task 001 Criterion baseline `task-001` ran at 2026-07-18T00:25:56Z on commit
  `7a4928c7113e7dce9157a78f972e07be70720e88` plus the task worktree, using rustc
  1.88.0 (`x86_64-pc-windows-msvc`), Windows 11 Pro 10.0.26100, and an AMD Ryzen 7
  8845HS (8 cores / 16 logical processors).
- Baseline estimates: candidate generation 3.1060 us; unmatched dispatch 6.0047 us;
  fan-out 1 at 6.2993 us, fan-out 32 at 10.617 us, and fan-out 256 at 43.152 us.
- Passed `cargo test --locked -p router-core`: 14 tests.
- Passed `cargo test --locked --workspace --all-features`: 24 tests.
- Passed `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`.
- Passed `cargo bench -p router-core --bench matcher -- --save-baseline task-001`.
- Passed `cargo fmt --all -- --check`.
- Passed `cargo check --locked --workspace --all-targets --all-features`.
- Passed `cargo clippy --locked --workspace --all-targets --all-features -- -D warnings`.
- Passed `cargo test --locked --workspace --all-features`: 12 tests passed.
- Passed `cargo test --locked --doc --workspace`: five crates, no doctests defined.
- Passed both `routerd --check-config` commands for the local and production example TOML.
- Passed `cargo build --locked --release --bin routerd`.
- Passed locked installation of `cargo-audit 0.22.2` under Rust 1.88 and
  `cargo audit` with 305 locked dependencies scanned.
- Passed `python scripts/validate-repo.py`: 118 files, 29 Rust, 12 TOML, 15 YAML,
  and 39 Markdown files validated.
- `docker build --pull -t kafka-edge-router:task-000 .` was attempted but could
  not run because no Docker-compatible CLI is installed; no container digest is available.
- The Kafka/WS/SSE manual smoke test remains deferred because Docker is unavailable.

## 17.07.2026

### Added

- Initial Rust workspace.
- Header-only Kafka routing metadata decoder.
- Indexed exact/wildcard matcher with bounded per-connection queues.
- WebSocket, SSE, gRPC, HTTP publish, and static webhook adapters.
- Health, status, metrics, local Kafka, deployment, CI, documentation, and ordered tasks.

### Known limitations

- The generated scaffold has not yet produced a committed `Cargo.lock`.
- Full Cargo type-checking is the first acceptance gate in task 000.
- Webhook retries are volatile across process restart.
- JWT/JWKS, direct TLS, and DNS-aware webhook SSRF protection are planned.
