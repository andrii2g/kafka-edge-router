# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e
FROM rust:1.88-bookworm@sha256:af306cfa71d987911a781c37b59d7d67d934f49684058f96cf72079c3626bfe0 AS build
WORKDIR /workspace
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential cmake libcurl4-openssl-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml clippy.toml ./
COPY crates ./crates
COPY tools ./tools
RUN cargo build --locked --release --bin routerd

FROM gcr.io/distroless/cc-debian12:nonroot@sha256:66aa873a4a14fb164aa01296058efd8253744606d72715e45acface073359faa AS runtime
COPY --from=build /workspace/target/release/routerd /usr/local/bin/routerd
COPY config/router.toml /etc/router/router.toml
USER 10001:10001
EXPOSE 8080 9090
ENTRYPOINT ["/usr/local/bin/routerd"]
CMD ["--config", "/etc/router/router.toml"]
