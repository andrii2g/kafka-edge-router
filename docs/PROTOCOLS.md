# Public protocol contracts

## Common principles

- Every connection is restricted to one authenticated tenant.
- A subscription id is unique only within its connection.
- Optional filter fields are exact matches; omitted fields are wildcards.
- Message ids are always exposed for deduplication.
- Queue capacity is bounded and a persistently slow consumer can be disconnected.
- Public protocol delivery is best effort unless a future durable mode says otherwise.

## WebSocket

Endpoint:

```http
GET /v1/ws?tenant_id=tenant-demo
Upgrade: websocket
```

In authenticated modes, use `Authorization: Bearer ...` or the trusted tenant header.
The query tenant is optional when authentication already resolves one, but any supplied
value must match.

Subscribe command:

```json
{
  "operation": "subscribe",
  "subscription_id": "news",
  "filter": {
    "kind": "content",
    "channel": "news"
  }
}
```

Acknowledgement:

```json
{
  "operation": "subscribed",
  "subscription_id": "news"
}
```

Unsubscribe command:

```json
{
  "operation": "unsubscribe",
  "subscription_id": "news"
}
```

Application ping:

```json
{
  "operation": "ping",
  "opaque": "request-17"
}
```

Delivery:

```json
{
  "operation": "message",
  "subscription_ids": ["news"],
  "message": {
    "metadata": {
      "message_id": "example-001",
      "tenant_id": "tenant-demo",
      "kind": "content",
      "type": "broadcast",
      "channel": "news",
      "content_type": "application/json",
      "source": {
        "topic": "router.input",
        "partition": 0,
        "offset": 42
      }
    },
    "payload": {
      "header": "Router online"
    }
  }
}
```

Application command failures produce a stable error envelope and do not close an otherwise
healthy connection:

```json
{
  "operation": "error",
  "code": "subscription_exists",
  "message": "subscription_id already exists"
}
```

| Code | Meaning |
|---|---|
| `invalid_json` | Text is not valid JSON |
| `invalid_command` | JSON does not match a supported command |
| `binary_not_supported` | Binary application commands are unsupported |
| `invalid_subscription_id` | Subscription id violates identifier rules |
| `invalid_filter` | Filter fields violate routing rules |
| `tenant_mismatch` | Requested filter tenant differs from the authenticated tenant |
| `subscription_exists` | Subscription id is already registered on this connection |
| `subscription_limit_reached` | Connection reached the configured subscription cap |
| `subscription_not_found` | Unsubscribe references an unknown id |
| `rate_limited` | Per-connection command budget is exhausted |
| `connection_closed` | Core registration no longer exists |
| `subscribe_failed` | Subscription could not be created for another stable reason |
| `unsubscribe_failed` | Subscription could not be removed for another stable reason |

Inbound frame and assembled-message sizes are capped by `api.ws_max_frame_bytes` and
`api.ws_max_message_bytes`. Oversized input closes with code `1009` and reason
`message_too_large`; other WebSocket protocol errors close with `1002` and reason
`protocol_error`. Core slow-consumer eviction closes with `1013` and reason
`slow_consumer`. Authentication or query-tenant failures reject the HTTP upgrade with
`401` or `403`, so no WebSocket registration is created.

Application text and binary commands share a fixed one-second per-connection budget set
by `api.ws_max_commands_per_second`. WebSocket protocol ping/pong frames are handled
separately. Per-message compression is intentionally disabled: no CPU and retained-memory
benchmark currently justifies enabling it.

## Server-Sent Events

Endpoint:

```http
GET /v1/events?tenant_id=tenant-demo&kind=content&channel=news
Accept: text/event-stream
```

The filter is fixed for the connection lifetime. Reconnect with a new URL to change it.
The `id` field is the message id. The event name is `kind.type`, `kind`, or `message`.
Data contains the same JSON delivery envelope used by WebSocket.

```text
id: example-001
event: content.broadcast
data: {"operation":"message",...}
```

Keep-alive comments are emitted at `api.sse_keep_alive_secs`. The endpoint returns
`Cache-Control: no-cache, no-transform`, `X-Accel-Buffering: no`, and
`X-SSE-Replay: unsupported`; application response compression is disabled.

The endpoint accepts only the documented filter fields, `subscription_id`, and
`queue_capacity`. Unknown fields, invalid filters, empty identifiers, and capacities
outside `1..=api.max_stream_queue_capacity` are rejected.

Browsers automatically send `Last-Event-ID` when reconnecting after receiving an event.
The router parses and ignores that header so native `EventSource` can reconnect, but it
does not replay the named event or any events missed while disconnected. A reconnect
starts a new live-only subscription. Clients must use each SSE `id` for deduplication and
must not treat reconnect as recovery of missed events.

## gRPC

Source schema:

```text
crates/router-proto/proto/router/v1/router.proto
```

Services:

- `Subscribe`: one fixed server stream;
- `Connect`: bidirectional commands and server events;
- `Publish`: raw byte payload to Kafka;
- `GetStatus`: operational counters.

In disabled auth mode, bidirectional `Connect` needs `auth.default_tenant` because no
first-message tenant is trusted before the stream is established. In authenticated
modes, metadata resolves the tenant.

Do not renumber or reuse protobuf fields. Additive fields should be optional where old
clients can safely omit them.

## HTTP publish

Endpoint:

```http
POST /v1/publish
Content-Type: application/json
```

Request:

```json
{
  "message_id": "example-001",
  "tenant_id": "tenant-demo",
  "kind": "content",
  "type": "broadcast",
  "channel": "news",
  "content_type": "application/json",
  "payload": {"ok": true}
}
```

The HTTP adapter serializes the JSON `payload` value. Use gRPC publish for arbitrary raw
bytes. A successful response means Kafka acknowledged the record:

```json
{
  "message_id": "example-001",
  "topic": "router.input",
  "partition": 0,
  "offset": 42
}
```

It does not mean any live client or webhook received the event.

## Static HTTP webhooks

Webhook destinations are configured, not dynamically registered through the public API.
One destination has one ordered worker. Requests contain:

```text
content-type: application/json
user-agent: kafka-edge-router/<version>
x-router-message-id: <message id>
idempotency-key: <message id>
x-router-attempt: <1-based attempt>
x-router-signature: sha256=<hex HMAC>    when configured
```

The body is the common JSON delivery envelope. Redirects are disabled. Successful 2xx
responses complete delivery. 408, 425, 429, and 5xx responses retry with bounded
exponential backoff. Other status codes are terminal.

## Stream queue allocation limit

Client-supplied `queue_capacity` values are rejected unless they are in the inclusive
range `1..=api.max_stream_queue_capacity`. Core independently enforces
`router.max_queue_capacity` for every live stream and webhook queue.
