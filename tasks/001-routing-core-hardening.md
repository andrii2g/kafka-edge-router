# Task 001: Harden the routing core

## Goal

Prove the route index remains correct under subscription churn, connection cancellation,
queue saturation, and wildcard overlap while preserving the bounded non-blocking hot path.

## Scope

- `crates/router-core/src/route.rs`
- `crates/router-core/src/router.rs`
- core unit/concurrency tests
- matcher benchmark scaffold
- architecture/status docs

## Required work

1. Add table-driven equivalence tests comparing indexed matches with
   `RouteFilter::matches` across every optional-dimension combination.
2. Add randomized/property tests for tenant isolation and candidate uniqueness.
3. Add deterministic concurrency tests for subscribe versus unregister, unsubscribe
   versus dispatch, and repeated drop guards.
4. Prove the serialized mutation path and conditional route-bucket cleanup cannot remove
   a concurrently repopulated bucket or accumulate unbounded empty keys; replace it only
   when a tested lower-contention design is demonstrably correct.
5. Prove duplicate subscription-id insertion is atomic per connection.
6. Define and test behavior when `slow_consumer_strikes` is zero or one.
7. Test the existing adapter and core queue-cap hierarchy, including zero, exact-limit,
   and over-limit values for live streams and static webhook workers.
8. Add a benchmark for candidate generation, unmatched dispatch, and fan-out.
9. Document any remaining lock contention risk.

## Invariants

- no async await while a map guard is held;
- no O(all subscriptions) scan;
- no tenant wildcard;
- no unbounded retry or queue; and
- no per-message spawned task.

## Acceptance criteria

- indexed and reference matcher agree for generated cases;
- Miri or Loom is evaluated and used where practical, with rationale if not selected;
- route-index cardinality returns to zero after churn tests;
- ThreadSanitizer-compatible tests are documented;
- benchmark commands and baseline output format exist; and
- all workspace gates pass.

## Required commands

```bash
cargo test --locked -p router-core
cargo test --locked --workspace --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo bench -p router-core --bench matcher -- --save-baseline task-001
python scripts/validate-repo.py
```

## Commit title

```text
feat(core): harden concurrent route index lifecycle
```
