# ADR 0005: Persist webhook delivery state in Kafka

- Status: accepted
- Date: 2026-07-18

## Context

The original webhook adapter registers one bounded core queue per destination and performs
HTTP attempts with in-memory sleeps. Source offsets can be committed before the worker
writes the request, and a process restart loses queued and sleeping deliveries. Calling
that mode durable would hide a real loss window.

Webhook delivery also cannot be exactly once. A receiver may process a request while the
router loses the HTTP response, or the router may crash after remote success and before
committing Kafka state.

## Decision

Webhook configuration selects exactly one process-wide mode:

- `volatile` retains the bounded in-memory worker for low-latency deployments that
  explicitly accept restart loss; and
- `durable` publishes matched commands to Kafka before the source record is committed.
  Durable mode does not run the volatile HTTP retry workers.

Durable mode uses three operator-created topics with the same partition count:

```text
router.webhook.delivery
router.webhook.retry
router.webhook.dead-letter
```

Every record is keyed by `destination_id`. Equal topic partition counts therefore place
one destination on the same partition number in all three topics. Each destination uses
`<group_id>.<destination_id>`; replicas join that group with the range assignor so one
member owns the destination partition number across delivery and retry topics. Only one
HTTP request per destination is in flight.
Configuration validation rejects equal topic names and empty ownership identities.

The version 1 JSON command contains:

```text
schema_version
delivery_id
destination_id
original_message_id
body_base64
attempt
next_attempt_at_ms
last_error_class
source_topic
source_partition
source_offset
state
```

The body is the existing public JSON delivery envelope. URLs, signing secrets, bearer
tokens, configured headers, and other destination credentials are never written to
Kafka. Workers resolve `destination_id` against local static configuration, retain the
original message id as both `idempotency-key` and `x-router-message-id`, and calculate
HMAC only immediately before an HTTP request.

Initial commands are broker-acknowledged before the source offset is committed. A partial
fan-out followed by a producer failure leaves the source uncommitted; replay may create
duplicate commands for destinations already published, but the original message id is
stable.

For a retryable result, the worker publishes the incremented attempt, next-at timestamp,
and bounded error class to the retry topic before committing the current record. It
pauses the destination delivery partition until that retry reaches a terminal result.
A permanent result or exhausted retry is published to the dead-letter topic before the
input is committed. Kafka publication failures stop the supervised component and leave
the input uncommitted.

## Failure outcomes

| Window | Restart outcome |
|---|---|
| before initial command acknowledgement | source record remains uncommitted and is replayed |
| after initial acknowledgement, before source commit | duplicate command is possible |
| before HTTP request | command remains in delivery/retry topic |
| after remote success, before Kafka commit | duplicate HTTP request is possible |
| after retry publication, before input commit | duplicate retry state is possible |
| while waiting for `next_attempt_at_ms` | persisted retry resumes after restart |
| before DLQ acknowledgement | input remains uncommitted and DLQ publication is retried |
| after DLQ acknowledgement, before input commit | duplicate DLQ record is possible |

There is no acknowledged-command loss window, but duplicates remain part of the contract.
Receivers must make `idempotency-key` durable. DLQ replay republishes the unchanged
command to the delivery topic only after the operator has corrected the destination or
receiver; replay tools must preserve the destination key and original message id.

## Retention and bounds

The delivery and retry topics use retention longer than the maximum retry horizon and
must not use compaction. The DLQ uses restricted access and operator-selected retention.
Serialized command size, attempts, destination count, destination queue size, HTTP
timeout, and Kafka producer timeout are all bounded. Payloads may exist in Kafka topics,
but secrets remain configuration-only.

## Consequences

- pending acknowledged deliveries survive router and worker restart;
- source throughput can now wait for Kafka acknowledgement in durable mode;
- duplicates are explicit at every external acknowledgement boundary;
- per-destination ordering depends on equal partition counts and stable destination ids;
- operators must provision, monitor, retain, and replay three topics; and
- volatile mode remains available but cannot be mistaken for durable delivery.

