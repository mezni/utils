FROM rust:1.95-slim-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
COPY services/driver-service/ ./services/driver-service/
COPY services/admin-service/ ./services/admin-service/

RUN cargo build --release --locked --package driver-service && \
    cp target/release/driver-service /driver-service

FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates libssl3 libpq5 curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /driver-service /driver-service

EXPOSE 8080

CMD ["/driver-service"]
