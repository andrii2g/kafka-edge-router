# Task 009: Harden identity, TLS, SSRF, and abuse controls

## Goal

Make the daemon safe for an untrusted network through cryptographic identity validation,
TLS, DNS-aware webhook controls, and protocol-level abuse limits.

## Required work

1. Add JWT validation with configured issuer, audience, algorithms, clock skew, and JWKS
   refresh/rotation. Reject algorithm confusion and unsigned tokens.
2. Map claims to tenant plus explicit subscribe/publish scopes.
3. Add optional mTLS identity for gRPC and internal deployments.
4. Add direct TLS configuration for HTTP/gRPC or document and test a mandatory protected
   proxy mode.
5. Add certificate/key reload without process restart where practical.
6. Implement a webhook DNS resolver that rejects every private/special resolved address,
   mitigates rebinding, and revalidates on connection.
7. Add egress proxy policy and destination port restrictions.
8. Add global/per-principal connection, subscription, command, and publish rate limits.
9. Add security-focused fuzz targets for headers, commands, URL parsing, and protobuf input.
10. Run dependency audit, secret scan, container scan, and threat-model review.

## Acceptance criteria

- cross-tenant access tests exist for every protocol;
- expired, wrong-audience, wrong-issuer, rotated, and malformed JWTs are tested;
- webhook DNS names resolving private addresses are rejected;
- public plaintext mode is impossible when production security mode is selected;
- rate limits are bounded and observable; and
- `docs/SECURITY.md` reflects the completed threat model.

## Commit title

```text
feat(security): enforce cryptographic identity and egress policy
```
