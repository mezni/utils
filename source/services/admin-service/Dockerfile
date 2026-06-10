FROM rust:1.95-slim-bookworm AS builder

WORKDIR /app
COPY source/Cargo.toml source/Cargo.lock ./
COPY source/crates/ ./crates/
COPY source/services/admin-service/ ./services/admin-service/
COPY source/services/driver-service/ ./services/driver-service/
COPY database/ /database/

RUN cargo build --release --locked --package admin-service && \
    cp target/release/admin-service /admin-service

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 libpq5 curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /admin-service /admin-service

EXPOSE 8081

CMD ["/admin-service"]
