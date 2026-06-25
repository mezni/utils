# Multi-stage build for Rust admin-service
FROM rust:1.88-slim as builder

WORKDIR /app

COPY Cargo.toml .
COPY Cargo.lock .
RUN --mount=type=cache,target=/root/.cargo/cache \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo fetch --locked

COPY src ./src
COPY src/**/*.rs ./src

RUN --mount=type=cache,target=/root/.cargo/cache \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/admin-service /app/

EXPOSE 3002

CMD ["/app/admin-service"]
