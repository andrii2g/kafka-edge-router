# ADR 0004: Expose at-least-once Kafka intake and best-effort live delivery

- Status: accepted
- Date: 2026-07-17

## Context

Kafka offsets, local queue acceptance, socket writes, remote processing, and application
acknowledgements are distinct state transitions. The MVP does not persist client cursors
or acknowledgements.

## Decision

Document Kafka intake as at least once and WebSocket/SSE/gRPC delivery as best effort.
Commit offsets after validation and local queue policy. Expose `message_id` everywhere
and require clients to deduplicate.

Webhook retry is bounded and volatile until a Kafka-backed delivery workflow is added.

## Consequences

- the contract matches implementable behavior;
- duplicates and loss windows are visible to consumers;
- low-latency live mode remains simple; and
- durable delivery must use a new opt-in contract rather than changing semantics silently.
