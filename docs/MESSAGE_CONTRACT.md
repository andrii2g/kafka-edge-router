# Kafka message contract

## Purpose

The routing plane is encoded in Kafka headers. Payload bytes remain opaque to the
matcher, which keeps routing latency independent of payload format and size.

## Required and optional headers

All header values are UTF-8 strings.

| Header | Cardinality | Limit | Notes |
|---|---|---:|---|
| `x-tenant-id` | required | 256 bytes | Security and routing boundary |
| `x-message-id` | optional | 256 bytes | Fallback is `topic:partition:offset` |
| `x-kind` | optional | 256 bytes | Domain category |
| `x-type` | optional | 256 bytes | Domain subtype |
| `x-channel` | optional | 256 bytes | Logical channel |
| `x-actor-id` | optional | 256 bytes | Producer actor |
| `x-audience-type` | paired | 256 bytes | Must appear with audience id |
| `x-audience-id` | paired | 256 bytes | Must appear with audience type |
| `x-content-type` | optional | 256 bytes | Defaults to octet-stream |

Identifiers may not be empty or contain control characters. Duplicate headers are not a
supported producer contract; the decoder currently uses the first matching header.
Task 002 adds an explicit duplicate-header rejection test or documents a chosen policy.

## Payload

The Kafka value is the application payload. The default maximum is 1 MiB and is
configurable through `kafka.consumer.max_payload_bytes`. Oversized messages are rejected
before the payload is copied into owned process memory.

`application/json` payloads are parsed only when a JSON destination needs an envelope.
The original bytes are always retained for gRPC and future binary adapters.

## Timestamp and source

The router retains Kafka's record timestamp when present and always adds source topic,
partition, and offset. Source coordinates are diagnostic metadata and must not replace a
producer-generated business id where idempotency spans topic rewrites or replay.

## Message key

Choose the key for the entity whose relative order matters.

Audience-specific event:

```text
tenant-a:team:team-17
```

Channel event:

```text
tenant-a:channel:news
```

Kafka only guarantees order within one partition. A producer changing key strategy can
change ordering and should be treated as a contract migration.

## Example

Kafka headers:

```text
x-message-id       example-001
x-tenant-id        tenant-demo
x-kind             content
x-type             broadcast
x-channel          news
x-audience-type    team
x-audience-id      team-17
x-content-type     application/json
```

Kafka key:

```text
tenant-demo:team:team-17
```

Kafka value:

```json
{
  "header": "Exercise update",
  "body": "Phase two has started."
}
```

## Compatibility rules

- Additive optional headers are backward compatible when old routers ignore them.
- Changing the meaning of an existing header requires a versioned topic or envelope.
- Do not move tenant identity into payload data.
- Do not rely on header order.
- Producers and consumers must treat `message_id` as opaque.
- Replays must retain original message ids unless replay is intentionally a new event.
