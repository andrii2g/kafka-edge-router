# Kubernetes deployment

These manifests are a secure-leaning starting point, not a turnkey production claim.
Replace image owner/tag, Kafka DNS, authentication, ingress, network-policy selectors,
resources, and secret management.

Apply in order:

```bash
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f secret.example.yaml   # only after replacing placeholders
kubectl apply -f deployment.yaml
kubectl apply -f service.yaml
kubectl apply -f pdb.yaml
kubectl apply -f hpa.yaml
kubectl apply -f network-policy.yaml
```

The Deployment derives a unique consumer group from the pod name to preserve full-stream
per-node consumption. A rolling update changes pod names and therefore starts new groups;
set `auto_offset_reset` and retention with that behavior in mind. Task 010 replaces this
example with a release-tested deployment strategy.

`trusted_header` mode is safe only when network policy and ingress prevent clients from
bypassing the authenticating proxy. The broad namespace selectors in the example must be
narrowed for a real cluster.
