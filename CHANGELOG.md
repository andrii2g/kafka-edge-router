# Changelog

All notable changes will be documented here. The project follows Semantic Versioning.



## 18.07.2026

### Added

- Committed `Cargo.lock` for the Rust 1.88 workspace baseline.
- Added exhaustive and deterministic randomized route-matcher properties, barrier-based subscription lifecycle race tests, queue-cap hierarchy coverage, and Criterion routing benchmarks.

### Changed

- Updated GitHub Actions to Node 24-compatible checkout and replaced the Node 20
  RustSec action with a locked `cargo-audit 0.22.2` installation.
- Installed libcurl development headers in Linux CI, release, and container builds for
  the vendored librdkafka build.
- Adapted route candidate inference, Kafka producer delivery receipts, protobuf generation,
  gRPC stream error propagation, and warning-level API drift to the locked dependency APIs.
- Enforced `--locked` in CI, release, Docker, and standard Make build commands.
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
