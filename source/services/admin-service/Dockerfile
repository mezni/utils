FROM rust:1.95-slim-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
COPY services/admin-service/ ./services/admin-service/
COPY services/driver-service/ ./services/driver-service/

RUN cargo build --package admin-service && \
    cp target/debug/admin-service /admin-service

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 libpq5 curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /admin-service /admin-service

EXPOSE 8081

CMD ["/admin-service"]
