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
| `x-recipient-type` | paired | 256 bytes | Must appear with recipient identity |
| `x-recipient-identity` | paired | 256 bytes | Must appear with recipient type |
| `x-content-type` | optional | 256 bytes | Defaults to octet-stream |

Identifiers may not be empty or contain control characters. Header names are matched
case-insensitively. Duplicate routing headers are rejected, including duplicates that
differ only by header-name case, so routing never depends on Kafka header order.

## Recipient routing

`x-recipient-type` and `x-recipient-identity` form one optional atomic routing dimension.
Both headers must be present together or both must be absent.

Recipient types are bounded, case-sensitive strings rather than a router-owned enum.
Producers can introduce domain categories without changing router code, for example:

| Recipient type | Recipient identity | Meaning defined by producer domain |
|---|---|---|
| `audience` | `abc123` | An audience aggregate |
| `team` | `321456` | A team |
| `superteam` | `bca321` | A higher-level team aggregate |

A subscription that omits both recipient fields is a recipient wildcard. A subscription
that supplies the pair requires both values to match exactly. Half-pair filters and
messages are invalid.

## Matching bound

The matcher has five logical optional dimensions: `kind`, `type`, `channel`, `actor_id`,
and the atomic recipient pair. Each populated dimension branches into an exact key and a
wildcard key. A fully populated message therefore produces at most `2^5 = 32` direct
route-index lookups, independent of the total number of unrelated subscriptions.

New recipient-type values do not add matcher dimensions and do not increase that bound.
Adding a new filterable field would increase the worst-case bound to `2^(n+1)` and
requires an explicit schema change, compatibility review, property tests, and benchmarks.
Runtime-defined routing keys and payload expressions remain unsupported.

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

Recipient-specific event:

```text
tenant-a:team:321456
```

Channel event:

```text
tenant-a:channel:news
```

The public publish API may accept an explicit ordering key. It is encoded as:

```text
tenant-a:explicit:<ordering-key>
```

The authenticated tenant prefix is mandatory and prevents cross-tenant key sharing.
Kafka only guarantees order within one partition. A producer changing key strategy can
change ordering and should be treated as a contract migration.

## Example

Kafka headers:

```text
x-message-id          example-001
x-tenant-id           tenant-demo
x-kind                content
x-type                broadcast
x-channel             news
x-recipient-type      team
x-recipient-identity  321456
x-content-type        application/json
```

Kafka key:

```text
tenant-demo:team:321456
```

Kafka value:

```json
{
  "header": "Exercise update",
  "body": "Phase two has started."
}
```

## Compatibility rules

- Additive optional headers are compatible only when ignoring them cannot broaden or
  weaken routing semantics.
- Changing the meaning of an existing header requires a versioned topic or envelope.
- `recipient_type` is an open vocabulary; adding a value does not change the schema.
- Protobuf recipient field numbers retain their previous wire positions; generated clients
  must regenerate for the new source names.
- Do not move tenant identity into payload data.
- Do not rely on header order.
- Producers and consumers must treat `message_id` as opaque.
- Replays must retain original message ids unless replay is intentionally a new event.
