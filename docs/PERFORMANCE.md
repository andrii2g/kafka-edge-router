# Performance qualification

## Reproducible commands

Run the matcher and bounded-dispatch benchmark from a clean, idle checkout:

```bash
cargo bench --locked -p router-core --bench matcher -- --save-baseline task-010
```

Run a bounded end-to-end sample against an already running router and Kafka broker:

```bash
cargo run --locked --release -p router-load -- \
  --tenant tenant-demo --channel load \
  --messages 10000 --rate-per-second 500 \
  --websocket-connections 8 --sse-connections 8 --grpc-connections 8 \
  --output artifacts/load-report.json
```

The generator uses fixed worker counts, a bounded publish queue, bounded connection and
message counts, bounded HTTP and drain timeouts, and fixed-size HDR histograms. It exits
nonzero after writing the report when a publish fails or any expected delivery is missing.
Use `--webhook-listen` only when the router is configured to call that receiver; combine it
with `--webhook-fail-every` for retryable failure injection.

## Representative scenarios

Record the exact command and vary one dimension at a time:

| Scenario | Payload | Connections | Rate | Fault |
|---|---:|---:|---:|---|
| protocol baseline | small JSON | 1 per live protocol | controlled | none |
| fan-out | 1 KiB | 32, 256, 1024 | controlled | none |
| retained memory | 1 MiB | bounded by deployment | controlled | slow readers |
| webhook recovery | 1 KiB | configured destinations | controlled | every fifth attempt fails |
| resilience soak | production mix | production replica count | expected peak | rebalance and rolling restart |

Always capture source commit, dirty-worktree state, Rust version, OS/kernel, CPU and memory,
Kafka version/partitions, router configuration, image digest, duration, and report files.
Do not compare runs that differ in topology or security mode as if they were equivalent.

## Task 010 local baseline

Measured on 2026-07-19 at commit `aa416e8bccf5b7e8821bc4e6072947f03a2bb6a0`
plus the Task 010 worktree, Rust 1.88.0, Windows 11 Pro build 26100, and an AMD Ryzen 7
8845HS (8 cores, 16 logical processors). Criterion used optimized Windows MSVC binaries.
Estimates below are the reported medians, not service-level objectives:

| Operation | Median |
|---|---:|
| candidate generation, 0 dimensions | 1.546 us |
| candidate generation, 6 dimensions | 3.065 us |
| unmatched dispatch, 128 B | 6.120 us |
| unmatched dispatch, 1 MiB | 6.767 us |
| accepted fan-out, 1 connection | 6.456 us |
| accepted fan-out, 32 connections | 10.630 us |
| accepted fan-out, 256 connections | 45.055 us |
| accepted fan-out, 1024 connections | 251.060 us |
| full-queue dispatch | 6.067 us |
| subscribe plus unsubscribe | 4.654 us |

A functional Linux container sample used Apache Kafka 4.3.1, one router replica, six topic
partitions, two connections per WS/SSE/gRPC protocol, 200 small JSON messages, and a
controlled 20 publishes/second. All 200 publishes succeeded and every protocol received
400 deliveries. End-to-end p50/p95/p99/p99.9 were 6.195/7.207/7.707/21.087 ms for WS,
6.191/7.191/7.683/21.087 ms for SSE, and 6.215/7.223/7.659/21.135 ms for gRPC. This short
sample proves tooling and contract behavior only; it is not a throughput ceiling or soak.

## Profiling

Attach to the release router PID on Linux while replaying the representative load:

```bash
./scripts/profile-router.sh cpu PID 120 artifacts/profiles/cpu
./scripts/profile-router.sh allocations PID 120 artifacts/profiles/allocations
./scripts/profile-router.sh locks PID 120 artifacts/profiles/locks
./scripts/profile-router.sh memory PID 600 artifacts/profiles/memory
```

CPU and lock modes require `perf`; allocation mode requires `heaptrack`. Store the tool
versions and load report beside the output. Profiles are environment evidence and are not
committed when they contain machine-specific paths or process details.

## Soak gate

`./scripts/soak-test.sh` defaults to four hours in five-minute phases. Each phase recreates
clients, every third phase introduces slow readers, and optional settings inject webhook
failures, Kafka restarts/rebalances, and Kubernetes rolling restarts. The summarizer rejects
publish failures and delivery gaps. Resource samples must show no unexplained monotonic RSS,
queue, connection, subscription, or lag growth after traffic stabilizes.

The multi-hour K3s soak and release-candidate game day are release gates. They require the
candidate image digest, the three namespace Secrets, observable Kafka, and retained evidence;
a local smoke run does not satisfy them.