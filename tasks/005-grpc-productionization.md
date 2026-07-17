# Task 005: Productionize gRPC streaming

## Goal

Add gRPC authentication interceptors, message/concurrency limits, reflection for local
use, health integration, and complete streaming lifecycle tests.

## Scope

- protobuf-compatible additive changes
- Tonic server configuration and interceptors
- gRPC integration tests
- protocol docs

## Required work

1. Move repeated auth enforcement into a tested interceptor or shared layer without
   weakening method-specific tenant checks.
2. Configure inbound/outbound message-size limits, concurrency limits, keep-alive, and
   load shedding.
3. Cap queue capacity on fixed subscriptions.
4. Add standard gRPC health service and optional reflection in non-production mode.
5. Test server stream and bidirectional subscribe/unsubscribe/ping/delivery.
6. Test invalid oneof, missing filter, tenant mismatch, duplicate subscription, cancellation,
   slow receiver, and publisher unavailable statuses.
7. Prove cancellation drops the registration guard.
8. Add generated-client integration tests to catch schema/codegen drift.
9. Preserve all existing protobuf field numbers.

## Acceptance criteria

- gRPC status codes are stable and documented;
- flow-control settings are explicit;
- no stream can request an unbounded queue;
- health and reflection exposure are configuration-controlled; and
- old generated clients remain source compatible for additive changes.

## Commit title

```text
feat(grpc): add limits interceptors and streaming tests
```
