# Release and rollback runbook

## Release boundary

Only an annotated or lightweight source tag matching `vMAJOR.MINOR.PATCH` or
`vMAJOR.MINOR.PATCH-rc.N` starts the release workflow. The tag base version must match
`workspace.package.version`, resolve to a committed source revision, retain a valid
`Cargo.lock`, and have a curated file under `docs/releases/`.

Do not create a tag until the corresponding commit is merged to `main`, all required CI
and security checks pass, performance evidence has been reviewed, and the game-day owner
has signed off. Moving or reusing a published tag is prohibited.

## Release qualification checklist

The release owner must retain command output, reports, image digests, and sign-off links
under `docs/release-evidence/<version>/` or in an immutable external evidence system.

### Before tagging

- [ ] The candidate commit is merged to `main` and the worktree is clean.
- [ ] Required CI, Kafka integration, dependency audit, secret scan, container build, and
  vulnerability-policy checks pass for that commit.
- [ ] Matcher and end-to-end scenarios are rerun with complete hardware, configuration,
  payload, fan-out, topology, and source metadata.
- [ ] CPU, allocation, lock-contention, and memory-retention profiles are reviewed with no
  unexplained hotspot or unbounded growth.
- [ ] A multi-hour K3s soak covers connection churn, slow readers, webhook failures, Kafka
  rebalances, and rolling restarts without unexplained RSS, queue, subscription, or lag
  growth.
- [ ] The candidate image runs as UID/GID `10001:10001`, passes configuration validation,
  and has no HIGH/CRITICAL vulnerability unless a scoped, approved exception exists.
- [ ] The Kubernetes overlay passes server-side validation with production-equivalent
  Secrets, Kafka connectivity, observability, resources, and disruption settings.
- [ ] The release-candidate game day completes every scenario below, including rollback,
  and all blocking findings are resolved.

### After the tag workflow

- [ ] Binary archives and the multi-architecture image are published from the exact source
  tag and committed lockfile.
- [ ] Checksums, SBOM, provenance attestations, and keyless signatures verify with the
  commands in this runbook.
- [ ] The immutable image digest is deployed to the candidate environment and recovery,
  readiness, lag, latency, and tenant-isolation checks pass.
- [ ] Release notes and the changelog identify the shipped version and retain only
  user-relevant changes and explicit operating boundaries.

A failed or missing item blocks release promotion. A short local smoke run can support a
checklist item but cannot replace the required K3s soak, profiles, rollback exercise, or
game day.

## Published artifacts

The workflow publishes:

- locked Linux `x86_64` and `aarch64` binary archives;
- one GHCR multi-architecture manifest addressed by tag and immutable digest;
- BuildKit SBOM and provenance attestations attached to the image;
- a release SPDX JSON SBOM covering downloadable artifacts;
- `SHA256SUMS` for binaries, image digest record, and release SBOM;
- keyless Cosign bundles for checksums and the release SBOM;
- GitHub build-provenance attestations for binaries and the image; and
- curated notes plus a bounded commit changelog.

The image gate builds from the committed lockfile, verifies UID/GID `10001:10001`, checks
its default configuration, and rejects HIGH or CRITICAL vulnerabilities unless the repository contains a scoped,
approved exception. Keyless signatures bind artifacts to the GitHub Actions OIDC identity;
no long-lived signing key is stored in repository secrets.

## Verification

```bash
gh release download v0.1.0-rc.1 --dir dist/v0.1.0-rc.1
cd dist/v0.1.0-rc.1
sha256sum --check SHA256SUMS
cosign verify-blob --bundle SHA256SUMS.bundle SHA256SUMS
cosign verify-blob \
  --bundle kafka-edge-router-v0.1.0-rc.1.spdx.json.bundle \
  kafka-edge-router-v0.1.0-rc.1.spdx.json
IMAGE="$(cat IMAGE-DIGEST)"
cosign verify \
  --certificate-identity-regexp '^https://github.com/andrii2g/kafka-edge-router/.github/workflows/release.yml@refs/tags/v' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "$IMAGE"
gh attestation verify routerd-v0.1.0-rc.1-x86_64-unknown-linux-gnu.tar.gz \
  --repo andrii2g/kafka-edge-router
gh attestation verify "oci://$IMAGE" --repo andrii2g/kafka-edge-router
```

Extract and execute `routerd --check-config` before deployment. Deploy the immutable image
digest from `IMAGE-DIGEST`, not a mutable tag.

## Kubernetes rollout

1. Create `kafka-edge-router-config`, `kafka-edge-router-identity`, and
   `kafka-edge-router-tls` Secrets through the cluster secret-management system.
2. Review the selected overlay, namespace, network selectors, resources, Kafka endpoints,
   issuer, and audience.
3. Deploy through `scripts/deploy-kubernetes.sh`, which verifies the release signature and
   attestation, injects the immutable digest, applies server-side, and checks the rollout.
4. Confirm every pod resolves a distinct Kafka group id ending in its `POD_UID`.
5. Check readiness, consumer lag, reconnect rate, queue-full outcomes, webhook retries,
   memory, and p99 latency through one full traffic cycle.

## Rollback

Select a previously verified release digest. Run:

```bash
KAFKA_EDGE_ROUTER_IMAGE=ghcr.io/andrii2g/kafka-edge-router \
  ./scripts/rollback-kubernetes.sh \
  "$KUBECONFIG" kafka-router sha256:REPLACE_WITH_64_HEX
```

The script rejects tags and malformed digests, updates only the `router` container, waits
for the rollout, and reads the deployed image back for equality. Configuration and
protobuf compatibility must be checked before rollback; an old binary cannot consume a
configuration containing unknown mandatory behavior.

Abort and investigate when readiness does not recover within ten minutes, Kafka lag grows
continuously, tenant-denial counters change unexpectedly, or the old image cannot parse the
current configuration. Do not bypass the PDB or force-delete healthy pods merely to make a
rollout progress.

## Game day

Record every command, UTC timestamp, source commit, image digest, cluster version, node
shape, Kafka topology, dashboard link, expected outcome, actual outcome, and follow-up.
The release candidate game day must cover:

1. one slow WS/SSE/gRPC reader and bounded eviction;
2. retryable and terminal webhook receiver failures;
3. a Kafka consumer rebalance;
4. one router process crash before and after offset commit windows;
5. a rolling restart while clients reconnect;
6. an invalid JWT and cross-tenant subscription attempt;
7. network denial of a private webhook target;
8. rollback from the candidate digest to the previous verified digest; and
9. recovery validation with stable message-id deduplication.

A short smoke run may validate tooling, but it does not satisfy the multi-hour soak or
release-candidate game-day gates. Open findings remain release blockers unless explicitly
accepted as documented operating boundaries by the release owner.
