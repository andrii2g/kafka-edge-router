# Changelog

All notable changes to this project are documented here. The project follows
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-07-19

### Added

- Tenant-isolated exact and wildcard routing from bounded Kafka headers.
- WebSocket, Server-Sent Events, gRPC, HTTP publishing, and static webhook interfaces.
- Explicit volatile and Kafka-backed durable webhook delivery with restart-safe retries
  and dead-letter handling.
- Health, readiness, status, Prometheus metrics, optional OTLP tracing, dashboards, and
  alerting examples.
- Container, Kubernetes, and systemd deployment assets with reproducible load, soak,
  security, and release workflows.

### Changed

- Defined extensible `recipient_type` and `recipient_identity` routing and compiled the
  pair as one bounded matcher dimension.
- Enforced bounded queues, payloads, subscriptions, connections, rates, retries, and
  protocol message sizes.
- Defined at-least-once Kafka ingestion, bounded best-effort live delivery, partition-local
  ordering, and message-id-based deduplication.
- Established one unique Kafka consumer group per router pod so subscriptions remain
  node-local without peer forwarding.
- Reorganized public documentation around a product overview, quick-start guide,
  audience-based index, and explicit design and operational boundaries.

### Security

- Added reloadable asymmetric JWT/JWKS validation, explicit subscribe and publish scopes,
  proxy-mTLS identity mapping, and loopback-only protected-proxy mode.
- Added webhook DNS validation and pinning, private and special-address rejection,
  redirect blocking, and destination host and port policy.
- Added dependency auditing, secret scanning, container vulnerability scanning, SBOMs,
  provenance attestations, checksums, and keyless artifact signatures.
- Enforced crates.io source and license policy, explicit cargo-vet trust records,
  SHA-pinned GitHub Actions, digest-pinned container inputs, and Dependabot review PRs.
- Added dependency-review gates and digest-only Kubernetes deployment with Cosign and
  GitHub provenance verification.
