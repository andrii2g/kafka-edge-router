# Testing strategy

## Test pyramid

### Unit tests

Use for pure validation, candidate expansion, reference matching, Kafka key selection,
HMAC signatures, retry classification, and URL rejection.

### Concurrency tests

Use barriers and deterministic channels for registration/unregistration races, queue-full
policy, shutdown cancellation, and route-index consistency. Avoid timing assertions based
only on sleeps.

### Adapter tests

Run Axum and Tonic against ephemeral listeners. Verify authentication, tenant rewriting,
commands, stream framing, body/message limits, and cleanup when clients disconnect.

### Kafka integration tests

Use the official Kafka container. Cover:

- required and optional headers;
- payload-size rejection;
- entity keys and partition ordering;
- commit behavior around valid and invalid records;
- producer acknowledgements;
- consumer restart and duplicate handling; and
- group topology assumptions.

### End-to-end tests

Start Kafka, routerd, a WS/SSE/gRPC client, and a webhook receiver. Publish one id and
assert all matching destinations see it, nonmatching tenants do not, and status/metrics
change as expected.

### Load and soak tests

Task 010 supplies a reproducible load generator. A soak should include connection churn,
slow consumers, webhook failures, Kafka rebalances, and rolling router restarts while
monitoring memory growth.

## Commands

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo test --doc --workspace
python scripts/validate-repo.py
```

Integration tests requiring Docker should be clearly marked and run in a separate CI job
so ordinary unit tests remain fast.

## Test data rules

- Never use production tokens, hosts, tenant names, or payloads.
- Use stable message ids in deterministic assertions.
- Make duplicate behavior explicit rather than expecting exactly one record unless the
  test controls the complete lifecycle.
- Keep payload fixtures small except in size-limit and performance tests.
- Assert negative tenant isolation in every public protocol suite.

## CI gates

A pull request cannot merge when formatting, Clippy, tests, repository validation, or
security audit fails. Temporary exceptions require an issue, owner, expiry date, and
narrowly scoped allowlist.
