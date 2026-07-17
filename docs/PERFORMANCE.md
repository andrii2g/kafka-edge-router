# Performance plan

## Performance hypothesis

Rust, Tokio, librdkafka, bounded MPSC queues, and shared `Bytes` make a suitable base, but
architecture dominates language choice. The expected gains come from:

- header-only matching;
- direct hash lookups rather than subscription scans;
- one payload copy from Kafka;
- one queue item per matched connection;
- no network await in the ingestion hot path; and
- bounded memory under slow consumers.

## Hot path

```text
recv -> payload-size check -> header lookup -> metadata validation
     -> candidate generation -> route bucket lookup -> connection coalescing
     -> bounded try_send -> async offset commit request
```

Benchmark these stages separately before optimizing protocol encoding or allocators.

## Required benchmark dimensions

- optional dimensions populated: 0, 2, 4, 6;
- total subscriptions: 1k, 10k, 100k, 1M where hardware permits;
- matching fan-out: 0, 1, 10, 100, 1k, 10k;
- payload size: 128 B, 1 KiB, 16 KiB, 256 KiB, 1 MiB;
- queue state: empty, partially full, full;
- subscription churn concurrent with dispatch;
- protocol: no writers, WS, SSE, gRPC, webhook;
- JSON and binary payloads; and
- single and multiple Kafka partitions.

## Metrics to report

- records per second;
- p50, p95, p99, and p99.9 decode/match/enqueue latency;
- end-to-end client latency by protocol;
- CPU utilization and scheduler profile;
- resident memory and bytes retained in queues;
- allocations per record;
- lock contention; and
- dropped/full/closed delivery rate.

Always report hardware, kernel, Rust version, optimization flags, Kafka topology, topic
partition count, message size, fan-out, and command line.

## Guardrails

Do not:

- add an unbounded channel to improve a synthetic throughput score;
- batch across tenant or ordering boundaries without documenting semantics;
- parse payloads in the matcher;
- spawn per-message tasks;
- use a custom allocator without a representative memory profile;
- pin threads or tune Tokio worker count without comparing defaults; or
- trade correctness during concurrent subscription updates for speed.

## Initial targets

Task 010 should establish targets from measured hardware rather than treating these as
promises. A useful first objective is to sustain the expected production peak at less
than 50% CPU with p99 route-and-enqueue latency below the application's budget and zero
queue-full outcomes for conforming clients.

## Optimization sequence

1. Measure the current matcher and queue fan-out.
2. Remove accidental payload copies and repeated serialization.
3. Add route-bucket and match-vector capacity hints based on profiles.
4. Evaluate explicit route shards owned by Tokio tasks if DashMap contention is visible.
5. Evaluate batched Kafka polling/commit behavior while preserving ordering.
6. Tune protocol write batching and HTTP/2 flow control.
7. Only then evaluate allocators, CPU affinity, or lower-level data structures.
