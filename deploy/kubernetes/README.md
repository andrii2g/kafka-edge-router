# Kubernetes deployment

The manifests use a Kustomize base and a release-candidate overlay:

```text
deploy/kubernetes/namespace.yaml       production namespace and restricted PSA labels
deploy/kubernetes/base/                workload, TLS proxy, service, PDB, HPA, policy
deploy/kubernetes/overlays/rc/         isolated kafka-router-rc validation overlay
deploy/kubernetes/router.toml.example  secret-backed production configuration template
```

The daemon binds only `127.0.0.1` in `protected_proxy` mode. An Envoy sidecar terminates
TLS on pod ports 8443 and 9443, removes proxy-identity headers supplied by clients, and
forwards to the loopback HTTP and gRPC listeners. Application JWT/JWKS validation remains
the identity boundary; TLS termination does not replace authentication.

## Required secrets

Create secrets through the cluster secret manager. For a manual RC setup, copy and edit
the template outside the repository, then use an administrator context:

```bash
cp deploy/kubernetes/router.toml.example /secure/path/router.toml
# Replace issuer, Kafka endpoints, and credentials in /secure/path/router.toml.
kubectl -n kafka-router-rc create secret generic kafka-edge-router-config \
  --from-file=router.toml=/secure/path/router.toml
kubectl -n kafka-router-rc create secret generic kafka-edge-router-identity \
  --from-file=jwks.json=/secure/path/jwks.json
kubectl -n kafka-router-rc create secret tls kafka-edge-router-tls \
  --cert=/secure/path/tls.crt --key=/secure/path/tls.key
```

The scoped deployment account intentionally cannot read or mutate these Secrets. Do not
commit rendered configuration, JWKS private keys, bearer tokens, Kafka passwords, or TLS
private keys. JWKS files contain public verification keys only, but mounting them through a
Secret keeps runtime identity changes under the same controlled workflow.

## Unique full-stream consumer groups

The base Kafka group id comes from `kafka.consumer.group_id`. The Downward API injects the
immutable pod UID as `POD_UID`; `kafka.group_id_suffix_env = "POD_UID"` appends and validates
that value at startup. A pod with UID `abc-123` therefore uses
`kafka-edge-router.abc-123`. Missing, malformed, or overlong suffixes fail configuration
before listeners start. Rolling updates intentionally create new groups; source retention
and `auto_offset_reset` must account for this topology.

## Render and validate

Production:

```bash
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl kustomize deploy/kubernetes
kubectl apply --server-side -k deploy/kubernetes
```

Isolated RC namespace:

```bash
kubectl kustomize deploy/kubernetes/overlays/rc
kubectl --kubeconfig "$KUBECONFIG" apply --dry-run=server \
  -k deploy/kubernetes/overlays/rc
kubectl --kubeconfig "$KUBECONFIG" apply --server-side \
  -k deploy/kubernetes/overlays/rc
```

Before a real rollout, replace the image tag with the verified digest from the release
`IMAGE-DIGEST` asset. Confirm the Kafka namespace/ports, Traefik and observability namespace
labels, webhook egress policy, resource requests, issuer/audience, certificate SANs, and
Secret names for the target cluster.

The default NetworkPolicy permits DNS, Kafka in a namespace named `kafka`, public HTTPS
excluding private/special IPv4 ranges, and ingress from K3s Traefik or observability. A
standard NetworkPolicy cannot express FQDN webhook allowlists; use an egress gateway or CNI
FQDN policy where that control is required. The application still revalidates and pins DNS
answers on every webhook attempt.

## Disruption and capacity

The three-replica baseline uses `maxUnavailable: 0`, a two-pod PDB, startup/readiness/live
probes through TLS, resource requests/limits, memory and CPU HPA targets, preferred
anti-affinity, and host/zone topology spread. `ScheduleAnyway` keeps the RC overlay usable
on single-node K3s; production operators should change it to `DoNotSchedule` only when every
required topology domain has enough capacity for surge rollouts.

Use [the release runbook](../../docs/RELEASE.md) for digest verification, rollout, game-day,
and rollback procedures.