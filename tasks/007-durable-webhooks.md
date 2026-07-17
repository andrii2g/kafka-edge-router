# Task 007: Add durable webhook delivery, retry, and dead-letter topics

## Goal

Replace volatile in-memory retry as the reliability boundary with Kafka-backed delivery
state while preserving per-destination ordering, idempotency, bounded concurrency, and
SSRF controls.

## Proposed topics

```text
router.webhook.delivery
router.webhook.retry
router.webhook.dead-letter
```

## Required work

1. Write a design ADR covering record schemas, keys, scheduling, attempts, retention,
   ordering, duplicate behavior, and operator replay.
2. Route matching publishes one delivery command per destination to the delivery topic;
   it does not perform HTTP.
3. Implement workers consuming destination-keyed records with bounded concurrency.
4. Persist attempt count, next-at timestamp, last error class, original message id, and
   destination id without storing secrets.
5. Implement bounded retry scheduling and terminal dead-letter publication.
6. Preserve `idempotency-key` and HMAC behavior.
7. Fence or coordinate destination ownership so two workers do not violate ordering.
8. Test process crash before request, after remote success before commit, during retry,
   and during DLQ publication.
9. Add metrics and runbook procedures for replaying DLQ records.
10. Keep an explicitly named `volatile` mode only if operators need it; do not silently
    mix modes.

## Acceptance criteria

- pending deliveries survive router/worker restart;
- every failure window has a documented duplicate or loss outcome;
- retries and DLQ are observable and bounded;
- destination secrets remain configuration-only; and
- integration tests prove recovery with real Kafka and an idempotent test receiver.

## Commit title

```text
feat(webhook): persist delivery retries and dead letters
```
