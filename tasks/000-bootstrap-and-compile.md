# Task 000: Bootstrap, compile, and lock the workspace

## Goal

Turn the generated implementation scaffold into a reproducibly compiling baseline on the
pinned Rust toolchain. Resolve real dependency API drift without weakening architecture or
lint gates.

## Non-goals

- no new features;
- no broad refactor;
- no delivery-semantic change; and
- no benchmark optimization.

## Inspect first

- root `Cargo.toml` and `rust-toolchain.toml`;
- every crate manifest;
- `VALIDATION.md`;
- CI workflows; and
- generated protobuf build configuration.

## Required work

1. Install/use Rust 1.88 with `rustfmt` and `clippy`.
2. Run `cargo generate-lockfile` and commit `Cargo.lock`.
3. Run `cargo check --workspace --all-targets --all-features`.
4. Fix only compile errors and clear API drift using current primary crate documentation.
5. Run formatting, Clippy with warnings denied, unit tests, doc tests, and repository
   validation.
6. Change Docker, CI, and release commands to use `--locked`.
7. Verify vendored `protoc` works without a system installation.
8. Run `routerd --check-config` against both checked-in TOML files.
9. Build the release binary and container.
10. Update `VALIDATION.md`, `docs/IMPLEMENTATION_STATUS.md`, and `CHANGELOG.md` with exact
    results.

## Acceptance criteria

- `Cargo.lock` exists and is committed;
- all workspace targets compile on Rust 1.88;
- no warning is suppressed globally to make Clippy pass;
- no `unsafe` is introduced;
- both configuration files validate;
- the Docker image builds from a clean context with `--locked`;
- CI uses `--locked`; and
- documentation no longer labels compilation as deferred.

## Required commands

```bash
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
cargo test --locked --doc --workspace
cargo run --locked -p routerd -- --config config/router.toml --check-config
cargo run --locked -p routerd -- --config config/router.production.example.toml --check-config
cargo build --locked --release --bin routerd
docker build --pull -t rust-kafka-edge-router:task-000 .
python scripts/validate-repo.py
```

## Manual verification

- Start local Kafka.
- Start the daemon.
- Run `scripts/smoke-test.sh`.
- Open one WS and one SSE example, publish an event, and verify both receive the same
  message id.

## Deliverable summary

Report dependency/API adjustments, exact commands, test counts, container digest, any
remaining warning, and all deferred items.

## Commit title

```text
build: establish compiling locked workspace baseline
```
