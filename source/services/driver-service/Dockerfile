FROM rust:1.95-slim-bookworm AS builder

WORKDIR /app
COPY source/Cargo.toml source/Cargo.lock ./
COPY source/crates/ ./crates/
COPY source/services/driver-service/ ./services/driver-service/
COPY source/services/admin-service/ ./services/admin-service/
COPY database/ ./database/

RUN cargo build --release --locked --package driver-service && \
    cp target/release/driver-service /driver-service

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 libpq5 curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /driver-service /driver-service

EXPOSE 8080

CMD ["/driver-service"]
