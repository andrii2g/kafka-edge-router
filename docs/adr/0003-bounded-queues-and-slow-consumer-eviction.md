# ADR 0003: Use bounded queues and evict persistent slow consumers

- Status: accepted
- Date: 2026-07-17

## Context

A real-time client can stop reading while Kafka continues producing. Awaiting that client
would couple ingestion latency to the slowest destination. An unbounded queue would turn
network slowness into process memory exhaustion.

## Decision

Every connection and webhook destination receives a bounded MPSC queue. The core hot path
uses `try_send`. Queue-full outcomes drop that delivery and increment a strike counter.
The connection is unregistered at the configured threshold.

## Consequences

- memory is bounded by configured cardinalities;
- live delivery is explicitly best effort;
- slow clients reconnect rather than accumulating unlimited lag; and
- guaranteed delivery requires a separate persisted mode.
