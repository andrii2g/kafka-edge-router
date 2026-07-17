# Task 002: Verify Kafka integration and commit semantics

## Goal

Create an integration suite that demonstrates the Kafka header contract, producer keys,
partition ordering, poison-record policy, restart duplicates, and consumer-group topology.

## Scope

- `crates/router-kafka`
- Docker-backed integration tests
- Kafka test fixtures and scripts
- delivery and operations docs

## Required work

1. Add a reusable Kafka test harness using the official pinned Kafka image.
2. Test required header absence, invalid UTF-8, audience pairing, identifier limits, and
   maximum payload size.
3. Decide and test duplicate-header behavior.
4. Test default message id and content type.
5. Test producer entity keys route equal entities to the same partition.
6. Test per-partition ordering through the core receiver.
7. Test valid and invalid-record commit policy.
8. Force consumer restart around the dispatch/commit boundary and demonstrate duplicate
   handling by message id.
9. Force a rebalance and record expected processing behavior.
10. Add optional dead-letter/quarantine design notes for malformed records.
11. Expose consumer rebalance and commit-error counters.

## Acceptance criteria

- integration tests run from one documented command;
- tests are isolated with unique topic/group names;
- no fixed sleeps where broker state can be polled;
- duplicate and loss windows match `docs/DELIVERY_SEMANTICS.md`;
- CI has a separate Kafka integration job; and
- local unit tests remain runnable without Docker.

## Required commands

```bash
cargo test --locked -p router-kafka
cargo test --locked --test kafka_integration -- --test-threads=1
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
python scripts/validate-repo.py
```

## Commit title

```text
test(kafka): prove header and offset semantics
```
