# Security model

## Trust boundaries

The daemon crosses four boundaries:

1. Kafka records from producers;
2. public HTTP/WebSocket/SSE clients;
3. public gRPC clients; and
4. outbound webhook destinations.

Tenant identity is security-sensitive. Routing metadata other than tenant is treated as
untrusted selection data and validated for size and control characters.

## Authentication modes

### disabled

Only for isolated development. It accepts a requested tenant or configured default. Do
not expose this mode to an untrusted network.

### static_bearer

Maps an opaque bearer token to one tenant. It is suitable for controlled environments
and bootstrap deployments but lacks token expiry, rotation metadata, issuer validation,
and granular scopes. Inject tokens through a secret-mounted file or secure configuration
system, not Git.

### trusted_header

Trusts a tenant header injected by an authenticated reverse proxy. The daemon must be
network-isolated so clients cannot bypass that proxy or supply the trusted header
directly. The proxy should strip incoming copies before adding its verified value.

Task 009 adds JWT/JWKS validation and optional mTLS identity mapping.

## Authorization

Authentication resolves a `Principal` with exactly one tenant. Protocol adapters:

- reject a request tenant that differs from the principal;
- rewrite subscription filters to the principal tenant; and
- register core connections using the principal tenant; and
- require the principal tenant in `auth.publish_tenants` before either publish API can
  invoke Kafka.

Publish permission is independent from subscription authentication. The current hook is
a tenant allowlist; future channel, audience, or action authorization belongs in that
explicit policy layer before `Router::subscribe` or publish. Do not encode authorization
as a payload filter.

## Input limits

Current controls include:

- HTTP body size;
- Kafka payload size;
- identifier length and control-character rejection;
- subscriptions per connection;
- bounded connection queues with a process-wide hard cap;
- webhook attempt, timeout, and backoff limits; and
- no arbitrary expression language.

Publicly requested queue capacities are capped by `api.max_stream_queue_capacity`, and
core rejects every queue above `router.max_queue_capacity`, including static webhooks.
WebSocket frame/message sizes and per-connection command rates are bounded. Per-message
compression remains disabled to avoid unmeasured CPU and retained-memory amplification.
Tasks 004/005 add remaining SSE and gRPC protocol-specific controls.

## TLS

The local listeners are plaintext. Production must terminate TLS either:

- in an ingress or sidecar with a protected network hop; or
- directly in the daemon after task 009 adds certificate configuration and rotation.

Webhooks require HTTPS unless the destination explicitly sets `allow_http = true`.
Reqwest uses rustls in this workspace configuration.

## Webhook SSRF controls

Implemented baseline:

- URL schemes restricted to HTTPS by default;
- embedded credentials rejected;
- fragments rejected;
- exact hostname allowlist;
- literal private, loopback, link-local, multicast, and unspecified addresses rejected;
- redirects disabled; and
- reserved security and framing headers cannot be overridden.

Known gap: a DNS hostname can resolve to a private address or change resolution between
validation and connection. Task 009 must add a resolver that validates every resolved
address, pins or revalidates the selected address, and handles DNS rebinding. Until then,
use operator-controlled allowlisted hostnames and egress firewall policy.

## Secrets

Secrets include:

- bearer tokens;
- Kafka SASL passwords;
- webhook signing secrets;
- TLS private keys; and
- future JWT client credentials.

They must not appear in logs, metrics, panic messages, repository history, or generated
manifests. Kubernetes examples use secret references and placeholders only.

## Logging and payload privacy

The code logs message ids and Kafka coordinates but not payload bodies. New telemetry
must classify high-cardinality and sensitive fields. Tenant id may itself be sensitive;
production operators should decide whether to hash or omit it from exported telemetry.

## Dependency and supply-chain controls

CI includes formatting, Clippy, tests, dependency audit, and container build workflows.
The committed `Cargo.lock` is enforced with `--locked`. Pin GitHub Actions to immutable commit SHAs
before a high-assurance release. Produce an SBOM and signed image provenance in task 010.

## Vulnerability reporting

Use the process in the root [`SECURITY.md`](../SECURITY.md). Do not open a public issue for
a vulnerability that enables cross-tenant access, credential disclosure, remote code
execution, SSRF, or denial of service.

