# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.88.0

FROM rust:${RUST_VERSION}-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        cmake \
        libcurl4-openssl-dev \
        libsasl2-dev \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/rustium

COPY Cargo.toml Cargo.lock LICENSE README.md ./
COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/src/rustium/target,sharing=locked \
    cargo build --locked --release --package rustium \
    && install -D -m 0755 target/release/rustium /out/rustium

FROM debian:bookworm-slim AS runtime

ARG VERSION=0.1.0-alpha.1
ARG REVISION=unknown
ARG CREATED=unknown

LABEL org.opencontainers.image.title="Rustium" \
      org.opencontainers.image.description="Standalone log-based Change Data Capture service" \
      org.opencontainers.image.source="https://github.com/ulnit/rustium" \
      org.opencontainers.image.documentation="https://github.com/ulnit/rustium#readme" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.created="${CREATED}"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libsasl2-2 \
        libssl3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 65532 rustium \
    && useradd --uid 65532 --gid rustium --no-create-home --home-dir /var/lib/rustium --shell /usr/sbin/nologin rustium \
    && install -d -o 65532 -g 65532 -m 0750 /etc/rustium /var/lib/rustium

COPY --from=builder /out/rustium /usr/local/bin/rustium

USER 65532:65532
WORKDIR /var/lib/rustium
VOLUME ["/var/lib/rustium"]
EXPOSE 8080

ENTRYPOINT ["rustium"]
CMD ["run", "--config", "/etc/rustium/rustium.yaml"]
