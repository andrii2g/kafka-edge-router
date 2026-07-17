# Task 010: Benchmark, soak, package, and release

## Goal

Establish repeatable performance evidence and a secure release pipeline suitable for the
first tagged production candidate.

## Required work

1. Implement matcher microbenchmarks and an end-to-end load generator for WS, SSE, gRPC,
   and webhook fan-out.
2. Define representative scenarios and record hardware/configuration metadata.
3. Run CPU, allocation, lock-contention, and memory-retention profiles.
4. Run a multi-hour soak with connection churn, slow clients, webhook failures, Kafka
   rebalances, and rolling restarts.
5. Resolve leaks, unbounded growth, and correctness failures before tuning.
6. Publish baseline p50/p95/p99/p99.9 and throughput results without overgeneralizing.
7. Make Kubernetes manifests production-valid, including unique full-stream group ids,
   secrets, TLS, disruption behavior, resources, and topology spread.
8. Build multi-architecture images, generate SBOM and provenance, scan them, and sign
   artifacts.
9. Add tagged release workflow, checksums, changelog generation, and rollback runbook.
10. Conduct a release-candidate game day and record findings.

## Acceptance criteria

- benchmarks are reproducible from documented commands;
- no known unbounded memory growth remains under soak;
- release artifacts are built from committed lockfile and source tag;
- image runs non-root and passes vulnerability policy;
- SBOM, checksums, signatures, and provenance are published;
- rollback is tested; and
- `v0.1.0-rc.1` release notes state delivery semantics and known limits precisely.

## Commit title

```text
release: establish benchmarked signed release pipeline
```
