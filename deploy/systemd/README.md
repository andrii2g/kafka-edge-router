# systemd installation

```bash
sudo useradd --system --home /nonexistent --shell /usr/sbin/nologin kafka-router
sudo install -m 0755 target/release/routerd /usr/local/bin/routerd
sudo install -d -m 0750 -o root -g kafka-router /etc/rust-kafka-edge-router
sudo install -m 0640 -o root -g kafka-router config/router.production.example.toml \
  /etc/rust-kafka-edge-router/router.toml
sudo install -m 0644 deploy/systemd/routerd.service /etc/systemd/system/routerd.service
sudo systemctl daemon-reload
sudo systemctl enable --now routerd
```

Replace all placeholders and configure TLS/network controls before exposing listeners.
