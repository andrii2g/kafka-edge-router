# syntax=docker/dockerfile:1.7
FROM rust:1.97-bookworm AS build
WORKDIR /workspace
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential cmake libcurl4-openssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml clippy.toml ./
COPY crates ./crates
RUN cargo build --locked --release --bin routerd

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /nonexistent --shell /usr/sbin/nologin router
COPY --from=build /workspace/target/release/routerd /usr/local/bin/routerd
COPY config/router.toml /etc/router/router.toml
USER 10001:10001
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/routerd"]
CMD ["--config", "/etc/router/router.toml"]
