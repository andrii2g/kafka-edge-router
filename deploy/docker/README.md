# Container notes

Build:

```bash
docker build -t rust-kafka-edge-router:local .
```

Run against Kafka on the host:

```bash
docker run --rm --network host \
  -v "$PWD/config/router.toml:/etc/router/router.toml:ro" \
  rust-kafka-edge-router:local
```

The Docker build copies the committed `Cargo.lock` and uses `cargo build --locked`.
The runtime image is non-root and contains only CA certificates, the binary, and a default
configuration file.
