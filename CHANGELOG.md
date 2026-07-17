# Changelog

All notable changes will be documented here. The project follows Semantic Versioning.

## 18.07.2026


## 17.07.2026

### Added

- Initial Rust workspace.
- Header-only Kafka routing metadata decoder.
- Indexed exact/wildcard matcher with bounded per-connection queues.
- WebSocket, SSE, gRPC, HTTP publish, and static webhook adapters.
- Health, status, metrics, local Kafka, deployment, CI, documentation, and ordered tasks.

### Known limitations

- The generated scaffold has not yet produced a committed `Cargo.lock`.
- Full Cargo type-checking is the first acceptance gate in task 000.
- Webhook retries are volatile across process restart.
- JWT/JWKS, direct TLS, and DNS-aware webhook SSRF protection are planned.
