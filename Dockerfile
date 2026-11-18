# Build the converter in a pinned Rust image, then ship only the binary and CA
# certificates. TLS is rustls-based; no OpenSSL runtime is required.
FROM rust:1.88-bookworm AS builder
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config cmake make g++ \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release --locked -p tsdb2parqtel

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --create-home parqtel
COPY --from=builder /src/target/release/tsdb2parqtel /usr/local/bin/tsdb2parqtel
USER 10001
ENTRYPOINT ["/usr/local/bin/tsdb2parqtel"]
CMD ["--help"]
