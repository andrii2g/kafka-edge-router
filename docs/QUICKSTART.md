# Quick start

This guide starts a local Kafka broker, runs Kafka Edge Router with development
configuration, opens a subscription, publishes an event, and executes the smoke test.

## Prerequisites

- Rust 1.88 or newer with `rustfmt` and `clippy`;
- Docker with Compose;
- `curl`; and
- optionally `grpcurl` and a modern browser for protocol examples.

Run all commands from the repository root.

### Windows and WSL

Codex and Windows-native tooling invoke Linux commands through the WSL distribution
configured by `WSL_DISTRIBUTION` in [`AGENTS.md`](../AGENTS.md). List installed
distributions from PowerShell:

```powershell
wsl --list --quiet
```

If the configured name does not match the local installation, update only the
`WSL_DISTRIBUTION` value in `AGENTS.md`.

## 1. Start Kafka

```bash
./scripts/dev-up.sh
```

The script starts Apache Kafka on `localhost:9092`, waits for readiness, and creates the
six-partition `router.input` topic.

## 2. Run the router

In a separate terminal:

```bash
cargo run --locked -p routerd -- --config config/router.toml
```

The development configuration exposes:

```text
HTTP / WebSocket / SSE  127.0.0.1:8080
public gRPC             127.0.0.1:9090
Kafka                   127.0.0.1:9092
```

It uses authentication mode `disabled` with tenant `tenant-demo`. Do not use this
configuration for a public deployment.

## 3. Subscribe

For SSE:

```bash
curl -N 'http://127.0.0.1:8080/v1/events?tenant_id=tenant-demo&kind=content&channel=news'
```

For WebSocket, open [`examples/websocket-client.html`](../examples/websocket-client.html)
and subscribe with:

```json
{
  "operation": "subscribe",
  "subscription_id": "news-for-team-17",
  "filter": {
    "kind": "content",
    "type": "broadcast",
    "channel": "news",
    "recipient_type": "team",
    "recipient_identity": "team-17"
  }
}
```

The browser SSE example is
[`examples/sse-client.html`](../examples/sse-client.html). gRPC commands are documented
in [`examples/grpcurl.md`](../examples/grpcurl.md).

## 4. Publish

```bash
./scripts/publish-example.sh
```

The publish API acknowledges the Kafka partition and offset. The Kafka consumer then
receives the record and routes it to matching subscribers.

## 5. Run the smoke test

With Kafka and the router running:

```bash
./scripts/smoke-test.sh
```

## 6. Stop the local environment

Stop the router with `Ctrl+C`, then stop Kafka:

```bash
./scripts/dev-down.sh
```

## Next steps

- [Public protocol contracts](PROTOCOLS.md)
- [Kafka message contract](MESSAGE_CONTRACT.md)
- [Delivery semantics](DELIVERY_SEMANTICS.md)
- [Production configuration](../config/README.md)
- [Operations guide](OPERATIONS.md)
