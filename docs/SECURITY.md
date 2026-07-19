# Security model

## Trust boundaries

The daemon crosses four hostile boundaries: Kafka records, public HTTP/WebSocket/SSE
clients, public gRPC clients, and outbound webhook destinations. Tenant identity is an
authorization boundary. Payloads never participate in routing or authorization.

| Threat | Control | Residual operator responsibility |
| --- | --- | --- |
| forged identity or cross-tenant access | JWT signature, issuer, audience, expiry, algorithm allowlist, tenant claim, explicit scopes, and filter tenant rewriting | protect the JWKS mount and issuer |
| stolen static credentials | production supports JWT rotation and proxy mTLS identity; secrets are never logged | rotate credentials and restrict secret mounts |
| public plaintext or trusted-header bypass | protected_proxy requires authenticated mode and loopback-only daemon listeners | expose only the TLS/mTLS proxy and strip identity headers |
| memory/CPU exhaustion | bounded queues, global/per-tenant connection and subscription caps, bounded principal table, command/publish fixed-window rates | size limits for expected load and alert on rejections |
| webhook SSRF or DNS rebinding | HTTPS by default, exact host and port policy, every resolved address checked, per-attempt DNS resolution pinned to the connection, redirects and environment proxies disabled | maintain egress firewall and DNS policy |
| dependency or image compromise | locked crates.io dependencies, cargo-deny, cargo-vet, SHA-pinned Actions, digest-pinned images, PR dependency review, release signatures and attestations | review dependency trust changes and deploy only verified digests |

## Identity and authorization

disabled is development-only. static_bearer maps an opaque token to one tenant.
trusted_header is valid only behind the protected proxy boundary.

jwt loads a bounded JWKS file at startup and reloads it at the configured refresh
interval. A failed refresh retains the last valid key set. Tokens require exp, exact iss,
configured aud, a bounded kid, and an allowed asymmetric algorithm. Supported algorithms
are RS256/384/512, ES256/384, and EdDSA; unsigned and symmetric tokens are rejected. The
configured tenant claim is authoritative. The scope claim must grant router.subscribe
and/or router.publish explicitly.

proxy_mtls trusts an identity header only on loopback listeners in protected_proxy mode.
The TLS proxy verifies the client certificate, strips any client-supplied identity
header, injects the verified identity, and maps it to a configured tenant and permissions.
Certificate and key reload belongs to that proxy, allowing rotation without restarting
the daemon. The daemon independently reloads JWKS keys.

Every protocol rejects a requested tenant that differs from the principal and rewrites
subscription filters to the authenticated tenant. Subscription permission never implies
publish permission.

## Production transport

The production example selects server.security_mode = protected_proxy. Configuration
validation rejects non-loopback HTTP or gRPC listeners and rejects disabled
authentication in this mode. Therefore the daemon cannot expose a public plaintext
listener under production security mode.

The proxy must:

1. terminate TLS for HTTP, WebSocket, SSE, and gRPC;
2. use mTLS when proxy_mtls identity is selected;
3. bind the daemon hop only through loopback in the same pod or host;
4. remove inbound tenant and identity headers before adding verified values; and
5. reload certificates and keys without dropping the protected listener.

## Abuse controls

router.max_connections and router.max_subscriptions are process caps. The corresponding
per-tenant values prevent one principal from consuming the whole budget. Per-connection
subscription and queue caps remain independently enforced.

HTTP/gRPC publish and WebSocket/gRPC command rates use one-second fixed windows with
global and per-principal limits. api.max_rate_limit_principals hard-bounds counter memory.
HTTP returns 429, gRPC returns RESOURCE_EXHAUSTED, and WebSocket returns the stable
rate_limited application error. Rejections increment
router_security_limit_rejections_total.

## Webhook egress

Webhook URLs reject credentials and fragments. HTTPS and default port 443 are the
defaults; HTTP and other ports require explicit destination configuration. Every DNS
answer is bounded, deduplicated, and rejected if any address is private, loopback,
link-local, documentation, benchmark, multicast, unspecified, reserved, or otherwise
special. IPv4-mapped IPv6 addresses receive the same checks.

A new redirect-disabled, no_proxy client is built for each attempt after resolution. All
validated addresses are pinned into that client, preventing a second resolver lookup
during connect and forcing retries to revalidate DNS. The daemon intentionally ignores
environment egress proxies because proxy-side resolution would bypass address validation.
Deployments requiring a corporate proxy must enforce equivalent destination resolution
and policy outside the daemon before that mode is added.

## Secrets and telemetry

Bearer tokens, Kafka passwords, webhook signing keys, TLS private keys, JWT signing keys,
payloads, and authorization headers must not enter logs, metrics, manifests, or repository
history. Logs use message ids and bounded operational metadata. Tenant ids can be
sensitive and should not be exported as unbounded metric labels.

## Security verification

The security workflow runs dependency review, cargo-deny, cargo-vet, cargo audit, Gitleaks
history scanning, and a Trivy scan of the built release image. See the
[supply-chain policy](SUPPLY_CHAIN.md) for trust semantics and update review. The fuzz
package supplies cargo-fuzz targets for Kafka header
decoding, command JSON, webhook URLs, and protobuf messages. JWT tests cover expiry,
issuer, audience, malformed tokens, algorithm confusion, scopes, and JWKS rotation.
Protocol suites retain positive and cross-tenant negative cases.

Report vulnerabilities through the private process in the root SECURITY.md.
