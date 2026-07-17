# Codex execution backlog

Complete tasks in order. Each task should normally be one pull request and one conventional
commit. Do not silently combine tasks; later tasks assume earlier acceptance criteria.

| Order | Task | Outcome |
|---:|---|---|
| 000 | [Bootstrap and compile](000-bootstrap-and-compile.md) | Lock dependencies and pass the first full toolchain gate |
| 001 | [Routing-core hardening](001-routing-core-hardening.md) | Prove matching and concurrent lifecycle correctness |
| 002 | [Kafka integration semantics](002-kafka-integration.md) | Test headers, commits, restart, and rebalance behavior |
| 003 | [WebSocket productionization](003-websocket-productionization.md) | Limits, rate control, compatibility tests |
| 004 | [SSE productionization](004-sse-productionization.md) | Stream framing, proxy behavior, cancellation tests |
| 005 | [gRPC productionization](005-grpc-productionization.md) | Interceptors, limits, reflection, streaming tests |
| 006 | [Publish API hardening](006-publish-api-hardening.md) | Authorization, byte modes, idempotency contract |
| 007 | [Durable webhook delivery](007-durable-webhooks.md) | Delivery/retry/DLQ topics and recovery |
| 008 | [Observability](008-observability.md) | Histograms, lag, traces, dashboards |
| 009 | [Security hardening](009-security-hardening.md) | JWT/JWKS, TLS, DNS-aware SSRF, rate limits |
| 010 | [Performance and release](010-performance-and-release.md) | Load tests, SBOM, signed images, release runbook |

Every task inherits `AGENTS.md`. When criteria conflict, the task may narrow scope but may
not relax a non-negotiable invariant without a new ADR and explicit review.
