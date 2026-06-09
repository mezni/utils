# syntax=docker/dockerfile:1
FROM rust:1.85-slim-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
COPY services/admin-service/ ./services/admin-service/

RUN cargo build --package admin-service --release && \
    cp target/release/admin-service /admin-service

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 libpq5 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /admin-service /admin-service

EXPOSE 8081

CMD ["/admin-service"]
