# Contributing

Start from an issue or a clearly defined change request that describes the problem,
resource bounds, failure semantics, compatibility impact, and acceptance criteria. Read
`AGENTS.md` before changing code.

## Development

```bash
./scripts/dev-up.sh
cargo run -p routerd -- --config config/router.toml
```

Before submitting:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
python scripts/validate-repo.py
```

Use conventional commits. Keep behavior changes, refactors, and dependency upgrades in
separate commits when they can be reviewed independently.

Changes to topology, delivery semantics, storage, protobuf compatibility, or security
boundaries require an ADR. Public protobuf fields are additive: never renumber or reuse a
field number.

Do not include secrets, production payloads, customer tenant names, or raw authorization
headers in tests, examples, logs, or issues.
