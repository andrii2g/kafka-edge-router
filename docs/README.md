# Documentation

This index groups Kafka Edge Router documentation by user goal. The root
[`README.md`](../README.md) provides the product overview; this page is the entry point
for detailed guidance.

## Start here

| Goal | Document |
|---|---|
| Run the router locally | [Quick start](QUICKSTART.md) |
| Understand the system design | [Architecture](ARCHITECTURE.md) |
| Integrate WebSocket, SSE, gRPC, HTTP, or webhooks | [Public protocol contracts](PROTOCOLS.md) |
| Produce valid Kafka records | [Kafka message contract](MESSAGE_CONTRACT.md) |
| Understand delivery guarantees and failure windows | [Delivery semantics](DELIVERY_SEMANTICS.md) |

## Deploy and operate

| Area | Document |
|---|---|
| Production configuration, health, capacity, upgrades, and incidents | [Operations guide](OPERATIONS.md) |
| Authentication, tenant isolation, abuse controls, and webhook SSRF | [Security model](SECURITY.md) |
| Metrics, traces, dashboards, and alert response | [Observability](OBSERVABILITY.md) |
| Durable webhook topics, monitoring, and DLQ replay | [Durable webhook operations](WEBHOOK_OPERATIONS.md) |
| Benchmarks, profiles, load, and soak qualification | [Performance qualification](PERFORMANCE.md) |
| Artifact verification, rollout, rollback, and game day | [Release and rollback](RELEASE.md) |
| Kubernetes manifests and overlays | [Kubernetes deployment](../deploy/kubernetes/README.md) |

## Engineer and contribute

| Area | Document |
|---|---|
| Development and review requirements | [Contributing](../CONTRIBUTING.md) |
| Test strategy and CI gates | [Testing strategy](TESTING.md) |
| Architectural decisions and consequences | [Architecture decision records](adr/README.md) |
| Automated contributor rules | [AGENTS.md](../AGENTS.md) |
| Reusable Codex contribution prompt | [Codex contribution prompt](contributing/CODEX_PROMPT.md) |

## Releases

- [v0.1.0-rc.1 release notes](releases/v0.1.0-rc.1.md)
- [Release-candidate evidence](release-evidence/README.md)
- [Changelog](../CHANGELOG.md)

Release notes describe one version. The architecture, delivery, security, operations, and
protocol documents define the maintained product contracts.
