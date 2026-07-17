# Architecture

## Goals

The router is a long-running edge delivery process optimized for predictable latency,
bounded memory, and a small routing hot path. It converts Kafka's durable partitioned
log into live application streams and isolated webhook workers.

Primary goals:

- route without deserializing application payloads;
- keep fan-out independent of total subscription count;
- isolate slow consumers from Kafka ingestion;
- preserve tenant boundaries at connection registration and subscription updates;
- make delivery semantics and failure windows explicit; and
- remain operationally simple for the first horizontal-scaling stage.

Non-goals for the first production release:

- arbitrary payload-expression evaluation;
- global ordering across Kafka partitions;
- exactly-once delivery to browsers or RPC clients;
- persisted dynamic subscriptions;
- executing user-provided plugins; and
- sharing one Kafka consumer group across router nodes.

## Component graph

```text
Kafka broker
  │
  │ BorrowedMessage
  ▼
router-kafka::KafkaIngestor
  │  validate size + decode headers + copy payload once
  ▼
router-core::RoutedMessage (Arc + Bytes)
  │
  ▼
router-core::Router
  ├── RouteKey candidate expansion
  ├── DashMap route buckets
  ├── per-connection match coalescing
  └── bounded try_send
        ├── router-api WebSocket task
        ├── router-api SSE task
        ├── router-api gRPC task
        └── router-webhook destination task
```

`routerd` constructs these components, binds listeners, exposes readiness only after
construction and binding succeed, and coordinates shutdown.

## Workspace boundaries

### router-core

Owns stable domain types and the transport-independent matcher. It knows that a
connection has a delivery protocol for metrics, but it does not import protocol server
libraries. Tokio bounded MPSC is deliberately allowed because queue semantics are part
of the routing design.

### router-kafka

Owns librdkafka configuration, record decoding, offset commits, Kafka message keys, and
public publish acknowledgements. It passes validated immutable messages to core.

### router-api

Owns authentication adapters, HTTP handlers, WebSocket commands, SSE event framing, and
gRPC conversion. Each live stream is a long-lived task that exclusively owns its socket
writer and bounded receiver.

### router-webhook

Models each static destination as a core connection. This gives webhooks the same route
index and backpressure behavior as live clients while keeping HTTP retries outside the
consumer loop.

### router-proto

Owns the source Protocol Buffer schema and generated client/server interfaces. Field
numbers are an external compatibility surface.

### routerd

Owns configuration loading, dependency construction, listeners, health gates, signal
handling, and component supervision. Business rules do not belong here.

## Message lifecycle

1. `StreamConsumer::recv` yields a borrowed Kafka record.
2. The ingestor records Kafka byte counters.
3. The decoder rejects an oversized payload before copying it.
4. Routing headers are located and decoded as UTF-8.
5. Core metadata validation enforces mandatory tenant, identifier limits, and audience
   pairing.
6. The payload is copied once into `Bytes`; metadata and payload become a
   `RoutedMessage`.
7. Core expands exact/wildcard route candidates.
8. Matching route buckets are read and coalesced by connection id.
9. One `Delivery` containing all matching subscription ids is offered to each bounded
   connection queue with `try_send`.
10. Queue-full policy increments a strike; a persistent slow consumer is unregistered.
11. The ingestor requests an asynchronous Kafka offset commit.
12. Protocol tasks encode and write independently.

The consumer never waits for a network destination. It can still experience CPU
backpressure from matching and librdkafka's own buffering; these are measurable and
bounded by process capacity rather than one slow client.

## Route index

A `RouteFilter` contains:

```text
tenant_id       mandatory and exact
kind            optional exact or wildcard
type            optional exact or wildcard
channel         optional exact or wildcard
actor_id        optional exact or wildcard
audience_type   optional exact or wildcard
audience_id     optional exact or wildcard
```

The route index maps a fully compiled `RouteKey` to:

```text
connection_id -> [subscription_id, ...]
```

For each populated optional message dimension, candidate generation branches into its
exact value and wildcard `None`. A fully populated message creates 64 candidates. A
message with only `kind` and `channel` creates four candidates. Tenant never branches.

This design makes matching proportional to candidate count plus actual matches. It does
not iterate through unrelated subscriptions.

### Concurrency model

`DashMap` shards the connection and route registries. Critical sections contain only
validation, hash-map updates, match collection, sender cloning/state updates, or
`try_send`. No async operation occurs while a map guard is held.

Registration and unregistration paths must remain idempotent. Protocol adapters use an
RAII guard so cancellation or an early socket error removes subscriptions.

Task 001 strengthens subscription/unsubscription race tests and route-bucket cleanup.

## Backpressure model

Every destination queue is bounded. Core uses `try_send` rather than awaiting capacity.
`router.max_queue_capacity` is the process-wide hard cap for every connection and static
webhook. Protocol adapters may impose a lower public cap.

Default policy:

```text
queue accepts message -> reset full-strike counter
queue full            -> drop this delivery, increment strike
strike threshold       -> unregister connection
queue closed           -> unregister connection
```

This policy favors low-latency current data over retaining an unbounded backlog. Durable
delivery requires a different persisted mode and cannot be added invisibly.

Possible future policies should be explicit per subscription:

- `live`: current best-effort behavior;
- `latest`: coalesce by entity and keep only newest state;
- `durable`: persisted consumer cursor and acknowledgements.

## Encoding

The original payload stays as bytes. JSON protocols parse payload as JSON only during
wire encoding when the content type is `application/json`. Invalid JSON or a non-JSON
content type is represented as base64. This parsing is outside the matcher.

WebSocket, SSE, and webhook adapters share the JSON delivery envelope. gRPC carries raw
payload bytes and typed metadata.

## Process lifecycle

Startup order:

1. parse and semantically validate configuration;
2. initialize tracing;
3. construct router, Kafka producer/consumer, and webhook workers;
4. bind HTTP and gRPC listeners;
5. spawn supervised components;
6. set readiness true.

Shutdown order:

1. receive `SIGINT`, `SIGTERM`, or an unexpected component exit;
2. set readiness false;
3. broadcast shutdown;
4. stop Kafka intake and graceful servers;
5. drain tasks until the configured deadline;
6. abort remaining tasks;
7. set liveness false and exit.

Kubernetes should provide a termination grace period longer than the daemon's internal
grace period.

## Horizontal scaling

### Stage 1: full stream per node

Each router node uses a unique Kafka consumer group. Every node receives every record and
routes only to local connections.

```text
Kafka topic
  ├── group router-node-a -> node A
  ├── group router-node-b -> node B
  └── group router-node-c -> node C
```

This avoids a distributed subscription registry and peer forwarding. It is the selected
MVP topology in ADR 0001.

### Stage 2: shared group with peer forwarding

Only introduce after measurement proves duplicated Kafka work is material. Nodes would
share one consumer group, advertise aggregate route counts to peers, and forward one
message per matching peer over long-lived internal gRPC streams.

The cluster registry should distribute only:

```text
node_id + route_key + subscriber_count + generation
```

It must not distribute individual client connections. This stage needs membership,
generation fencing, retransmission policy, peer backpressure, and failure tests.

## Failure domains

- Kafka unavailable: consumer/producer log errors; readiness policy can later include
  broker health. Current readiness represents constructed and bound components.
- One live client slow: only that bounded queue drops and eventually disconnects.
- One webhook slow: only that destination worker retries and its queue can saturate.
- Protocol server exits: supervisor initiates process shutdown.
- Invalid Kafka record: logged and optionally committed to avoid a poison-record loop.
- Process crash after offset commit: a live delivery can be lost.
- Process crash before offset commit: a Kafka record can be redelivered.

## Extension points

Safe extension points include:

- an authorization policy called before core subscription;
- a durable webhook dispatcher backed by Kafka topics;
- OpenTelemetry spans around decode, match, enqueue, and network write;
- a route-index actor implementation if benchmarks show lock contention;
- peer forwarding behind a separate internal crate and ADR; and
- a durable subscription mode with an independent protocol contract.
