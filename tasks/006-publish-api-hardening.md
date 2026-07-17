# Task 006: Harden HTTP and gRPC publishing

## Goal

Make publishing a precise authenticated contract for JSON and raw bytes, with explicit
idempotency, size, key, and error behavior.

## Scope

- `MessagePublisher` contract
- HTTP/gRPC publish handlers
- Kafka publisher tests
- authorization policy hook

## Required work

1. Define publish permissions separately from subscribe permissions.
2. Require or generate message ids according to documented idempotency policy.
3. Add HTTP support for either JSON payload or explicitly encoded base64 bytes; reject
   ambiguous requests.
4. Enforce payload-size limits before Kafka allocation/send.
5. Validate content type and audience pairing consistently across protocols.
6. Allow an explicit ordering key only under a safe, documented policy; otherwise keep
   derived keys.
7. Map Kafka timeout, queue-full, authorization, validation, and unavailable errors to
   stable HTTP/gRPC responses.
8. Test producer idempotence configuration and broker acknowledgement response.
9. Add audit-safe publish counters without logging payloads.

## Acceptance criteria

- HTTP and gRPC publish semantics agree;
- caller cannot publish for another tenant;
- raw and JSON modes round-trip exactly;
- retries with one message id are documented and tested; and
- response never implies downstream delivery.

## Commit title

```text
feat(publish): define secure idempotent publish contract
```
