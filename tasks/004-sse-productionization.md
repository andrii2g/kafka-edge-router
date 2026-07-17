# Task 004: Productionize SSE delivery

## Goal

Prove correct SSE framing, keep-alive behavior, cancellation cleanup, proxy compatibility,
and the documented non-replay semantics.

## Scope

- SSE handler and configuration
- HTTP integration tests
- example and proxy documentation

## Required work

1. Cap queue capacity and validate all query parameters.
2. Test event id, event name, data framing, JSON escaping, and multiline payload behavior.
3. Test keep-alives without relying on long wall-clock sleeps.
4. Test disconnect/cancellation unregisters core state.
5. Add headers appropriate for disabling intermediary buffering where needed.
6. Document reverse-proxy idle timeout and buffering settings for common ingress classes.
7. Explicitly parse but reject or ignore `Last-Event-ID` with a documented response until
   durable replay exists; do not imply replay.
8. Add cross-tenant and slow-consumer tests.
9. Verify response compression is not causing harmful buffering.

## Acceptance criteria

- browser and curl examples match tests;
- message ids survive as SSE ids;
- no reconnect claim exceeds implementation;
- proxy configuration is documented; and
- cleanup leaves no active connection or subscription.

## Commit title

```text
feat(sse): harden framing lifecycle and proxy behavior
```
