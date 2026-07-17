# Changelog

All notable changes will be documented here. The project follows Semantic Versioning.

## 18.07.2026

### Added

- Committed `Cargo.lock` for the Rust 1.88 workspace baseline.

### Changed

- Updated GitHub Actions to Node 24-compatible checkout and replaced the Node 20
  RustSec action with a locked `cargo-audit 0.22.2` installation.
- Installed libcurl development headers in Linux CI, release, and container builds for
  the vendored librdkafka build.
- Adapted route candidate inference, Kafka producer delivery receipts, protobuf generation,
  gRPC stream error propagation, and warning-level API drift to the locked dependency APIs.
- Enforced `--locked` in CI, release, Docker, and standard Make build commands.
- Made repository validation ignore generated build output and validate shell scripts
  correctly on Windows; shell scripts are now tracked as executable.

### Validation

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
- `docker build --pull -t rust-kafka-edge-router:task-000 .` was attempted but could
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
