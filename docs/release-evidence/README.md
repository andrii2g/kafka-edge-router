# Release-candidate evidence

Create one directory per candidate, for example `v0.1.0-rc.1/`, and retain links or small
text/JSON summaries needed to review the gate. Large profiles and dashboards belong in the
approved artifact store, with immutable links recorded here.

The candidate record must contain:

- source tag, commit, lockfile hash, image digest, SBOM and provenance identifiers;
- runner, cluster, node, Kafka, ingress, and load-generator metadata;
- benchmark and multi-hour soak commands plus reports;
- CPU, allocation, lock, and memory profile findings;
- game-day timeline and expected versus actual outcomes;
- vulnerability scan and signature verification results;
- tested previous and candidate rollback digests; and
- every finding, owner, disposition, and approval timestamp.

Never commit kubeconfigs, bearer tokens, signing material, TLS keys, Kafka credentials, or
unredacted authorization headers.