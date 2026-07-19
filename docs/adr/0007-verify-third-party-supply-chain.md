# 0007: Verify third-party supply-chain inputs

## Status

Accepted on 2026-07-19.

## Context

Version tags identify a convenient release name but can move. Vulnerability scans detect
known findings but do not prove publisher identity, build provenance, or source review.
Lockfile checksums protect Rust crate integrity but do not establish that crate code is
appropriate to trust.

## Decision

Pin GitHub Actions and container images to immutable identities. Restrict Rust dependencies
to crates.io, enforce lockfile, advisory, and license policy, and use cargo-vet to record
audits and explicit baseline exemptions. Produce signed, attested release artifacts and
make the supported Kubernetes deployment path verify both claims before applying an image
digest.

Automated updates must arrive through pull requests. Vulnerability ignores require a
documented, scoped, expiring exception. CODEOWNERS records ownership, while automated
checks remain the merge gate until a second maintainer can provide independent approval.

## Consequences

Updates require deliberate SHA or digest changes and policy checks. Cargo-vet exemptions
are visible trust debt and must not be described as audits. A compromised upstream that
retains its signing identity is still possible, so maintainer review and periodic
reassessment remain necessary. Operators bypassing the deployment script must provide an
equivalent admission policy.

## Revisit trigger

Revisit when the project gains a second maintainer, adopts a cluster admission controller,
changes package registries, or can replace a material portion of the baseline exemptions
with independent audits.
