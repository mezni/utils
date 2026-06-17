FROM rust:1.80-slim-bookworm AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/

RUN mkdir -p services/admin-service/src && echo "fn main() {}" > services/admin-service/src/main.rs
RUN cargo build --release -p admin-service 2>/dev/null || true

COPY services/ ./services/
RUN cargo build --release -p admin-service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

RUN groupadd -r bornemap && useradd -r -g bornemap bornemap

COPY --from=builder --chown=bornemap:bornemap /app/target/release/admin-service /usr/local/bin/admin-service

USER bornemap
CMD ["admin-service"]
