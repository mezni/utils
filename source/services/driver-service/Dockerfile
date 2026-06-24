# Multi-stage build for Rust driver-service
FROM rust:1.81-slim as builder

WORKDIR /app

# Copy dependencies first for caching
COPY driver-service/Cargo.toml .
COPY driver-service/Cargo.lock .
RUN --mount=type=cache,target=/root/.cargo/cache \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo fetch --locked

# Copy source code
COPY driver-service/src ./src
COPY driver-service/src/**/*.rs ./src

# Build the binary
RUN --mount=type=cache,target=/root/.cargo/cache \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release

# Run sqlx prepare for offline mode
RUN --mount=type=bind,from=docker.io/postgres:16-alpine,src=/var/lib/postgresql/data,target=/var/lib/postgresql/data,readonly \
    --mount=type=bind,from=docker.io/postgres:16-alpine,src=/docker-entrypoint-initdb.d,target=/docker-entrypoint-initdb.d,readonly \
    sqlx prepare --database-url postgresql://bornemap:bornemap@postgres:5432/bornemap -- --db-url postgresql://bornemap:bornemap@postgres:5432/bornemap

# Production stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/driver-service /app/

# Copy sqlx offline data if exists
COPY --from=builder /app/.sqlx /app/.sqlx

# Expose port
EXPOSE 3001

# Run the binary
CMD ["/app/driver-service"]
