# Delivery semantics

## Terminology

- **accepted locally**: core completed matching and applied bounded queue policy;
- **written**: a protocol task completed a socket or HTTP write call;
- **acknowledged by Kafka**: the producer received broker delivery coordinates;
- **committed offset**: the consumer requested an offset commit for a consumed record;
- **delivered to application**: the remote application processed the event, which the
  live protocols cannot prove without an application acknowledgement.

These states are not interchangeable.

## Kafka to router

The consumer disables auto commit and auto offset store. For a valid record it:

1. decodes and validates metadata;
2. dispatches according to local bounded queue policy; and
3. requests an asynchronous commit for the record.

A crash before commit can cause redelivery. A crash after commit but before socket write
can cause live-delivery loss. Therefore Kafka-to-router processing is at least once, but
router-to-live-client delivery is best effort.

Invalid records are committed when `commit_invalid_messages = true`. This prevents a
poison record from looping forever but means correction requires republishing or replay
from a separate remediation process. When the setting is `false`, the consumer stops at
the malformed record. Stopping is required because Kafka commits are cumulative within a
partition: processing and committing a later record would otherwise skip the poison
record despite the configured policy. The daemon treats this component exit as a failure.

Production deployments should alert on invalid-record and commit-error counters and retain
enough source data to diagnose them. A quarantine topic is an optional operational
extension, not an automatic router behavior: a remediation process may copy the original
key, value, headers, source coordinates, decode error category, and first-seen timestamp
to a restricted-retention topic. It must preserve `x-message-id` when present, avoid
logging payloads, and publish successfully before the source offset is committed. That
workflow is at least once and quarantine consumers must deduplicate.

### Kafka configuration invariants

The `[kafka.consumer.properties]` and `[kafka.producer.properties]` tables permit
additional librdkafka tuning. The daemon applies those free-form properties first, then
reapplies its semantic invariants. Consequently, free-form settings cannot override:

- consumer bootstrap servers, group id, or client id;
- `enable.auto.commit = false`;
- `enable.auto.offset.store = false`;
- the validated `auto.offset.reset` value;
- producer bootstrap servers or client id;
- the configured producer delivery timeout;
- `enable.idempotence = true`; or
- `acks = all`.

This ordering prevents an operator-supplied property from silently weakening the delivery
contract documented here. New invariant-bearing settings must follow the same precedence
rule and need configuration tests.

## Live protocols

WebSocket, SSE, and gRPC queues are in memory. A queue-full result drops that delivery for
that connection. After the configured number of full outcomes, the connection is
unregistered and its protocol task eventually observes a closed receiver or failed
socket. WebSocket clients receive close code 1013 with reason slow_consumer when
the core evicts their saturated queue; the close frame is diagnostic, not a delivery
acknowledgement.

No live protocol currently persists:

- subscriptions;
- unacknowledged messages;
- a client cursor; or
- a replay window.

Clients must reconnect, resubscribe, and deduplicate by message id.

## Ordering

Kafka records are observed in partition order by a consumer. The router dispatches each
record synchronously before receiving the next from the stream loop. A connection queue
therefore receives records in the order the consumer processes them.

Ordering can still differ across:

- distinct Kafka partitions;
- router nodes using independent consumer groups;
- webhook destinations with different retry delays; and
- a client reconnecting to another router node.

No global order is promised.

## Duplicate scenarios

Duplicates can occur when:

- a consumer crashes after routing but before committing;
- an asynchronous offset commit is not persisted before rebalance;
- a producer retries without idempotence outside this router;
- an operator replays a topic; or
- a webhook times out after the remote service processed the request.

Use `message_id` and webhook `idempotency-key` to make application handling idempotent.

## Webhooks

Current webhook retry is bounded but volatile:

```text
maximum attempts
initial backoff
exponential factor 2
maximum backoff
request timeout
bounded destination queue
```

A process restart loses queued or sleeping deliveries. Task 007 introduces a durable
delivery topic, retry scheduling, attempt metadata, and dead-letter topic. Only after
that work and its recovery tests may documentation describe webhook delivery as durable.

## Publishing

HTTP or gRPC publish returns after the idempotent Kafka producer receives partition and
offset. It proves broker acknowledgement, not consumption or downstream delivery.

The producer uses a stable entity key selected from audience, channel, or the tenant. A
validated explicit ordering key is allowed and is always prefixed with the authenticated
tenant. Kafka order remains partition-local.

When `message_id` is omitted, the API generates a UUID before invoking the publisher.
Callers retrying one logical event should generate and reuse their own stable id,
especially when the acknowledgement response can be lost. Reusing an id does not make
the public API a deduplicating store: each broker-acknowledged call may append a record.
Consumers and webhook receivers use the stable id for idempotent processing.

Payload size, MIME syntax, audience pairing, ordering-key syntax, and routing identifiers
are validated before Kafka send. HTTP JSON and base64 forms and gRPC raw bytes converge
on the same transport-neutral command. Kafka queue saturation and acknowledgement timeout
remain distinct public failures.

## Future durable-client mode

A durable live-client mode requires a separate contract with at least:

- stable consumer identity;
- monotonic sequence or persisted source cursor;
- acknowledgement command;
- acknowledgement storage;
- lease/session ownership and fencing;
- replay retention and limits;
- resumption token validation;
- explicit behavior when retention is exceeded; and
- tests for crash before/after every state transition.

It must be opt-in and must not silently change the low-latency live mode.
