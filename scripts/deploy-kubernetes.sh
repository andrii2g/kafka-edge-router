#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "usage: $0 <kubeconfig> <namespace> <sha256:digest> [kustomize-overlay]" >&2
  exit 2
}

[[ $# -ge 3 && $# -le 4 ]] || usage

kubeconfig=$1
namespace=$2
digest=$3
overlay=${4:-deploy/kubernetes}
repository=andrii2g/kafka-edge-router
image=ghcr.io/${repository}

[[ -f "$kubeconfig" ]] || { echo "kubeconfig not found: $kubeconfig" >&2; exit 2; }
[[ "$namespace" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] ||
  { echo "invalid namespace: $namespace" >&2; exit 2; }
[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] ||
  { echo "expected sha256:<64 lowercase hex characters>" >&2; exit 2; }
[[ "$digest" != "sha256:0000000000000000000000000000000000000000000000000000000000000000" ]] ||
  { echo "the fail-closed placeholder digest cannot be deployed" >&2; exit 2; }

for command in cosign gh kubectl; do
  command -v "$command" >/dev/null ||
    { echo "required command not found: $command" >&2; exit 2; }
done

image_ref="${image}@${digest}"
identity='^https://github.com/andrii2g/kafka-edge-router/.github/workflows/release.yml@refs/tags/v'

cosign verify \
  --certificate-identity-regexp "$identity" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "$image_ref" >/dev/null

gh attestation verify "oci://${image_ref}" --repo "$repository" >/dev/null

overlay=${overlay%/}
[[ -f "$overlay/kustomization.yaml" ]] ||
  { echo "kustomization not found: $overlay/kustomization.yaml" >&2; exit 2; }

overlay_parent=$(dirname "$overlay")
overlay_name=$(basename "$overlay")
render_dir=$(mktemp -d "$overlay_parent/.${overlay_name}.digest-deploy.XXXXXX")
rendered=$(mktemp)
cleanup() {
  rm -f "$rendered" "$render_dir/kustomization.yaml"
  rmdir "$render_dir"
}
trap cleanup EXIT

cat >"$render_dir/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: $namespace
resources:
  - ../$overlay_name
images:
  - name: $image
    newName: $image
    digest: $digest
EOF

kubectl --kubeconfig "$kubeconfig" kustomize "$render_dir" >"$rendered"
grep -Fq "image: $image_ref" "$rendered" ||
  { echo "rendered deployment does not contain $image_ref" >&2; exit 1; }

kubectl --kubeconfig "$kubeconfig" --namespace "$namespace" \
  apply --server-side --field-manager kafka-edge-router-release -f "$rendered"
kubectl --kubeconfig "$kubeconfig" --namespace "$namespace" \
  rollout status deployment/kafka-edge-router --timeout=10m

deployed=$(kubectl --kubeconfig "$kubeconfig" --namespace "$namespace" \
  get deployment kafka-edge-router \
  -o jsonpath='{.spec.template.spec.containers[?(@.name=="router")].image}')

[[ "$deployed" == "$image_ref" ]] ||
  { echo "deployed image mismatch: expected $image_ref, got $deployed" >&2; exit 1; }

echo "deployed and verified: $deployed"
