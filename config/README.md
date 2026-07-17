# Configuration

`routerd` loads one TOML file and overlays environment variables whose names follow
`ROUTER__SECTION__FIELD`. `RUST_LOG` overrides `logging.filter`.

Examples:

```bash
export ROUTER__SERVER__HTTP_ADDR=127.0.0.1:8080
export ROUTER__KAFKA__CONSUMER__BROKERS=kafka.internal:9092
export RUST_LOG=routerd=debug,router_core=trace
cargo run -p routerd -- --config config/router.toml
```

Arrays and maps are clearer and less error-prone in TOML, so keep topics, Kafka
properties, bearer-token mappings, and webhook destinations in a mounted file.
Run `routerd --check-config` before deploying a change.
