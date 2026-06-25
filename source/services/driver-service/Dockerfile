# Multi-stage build for Rust driver-service
FROM rust:1.88-slim as builder

WORKDIR /app

# Copy dependencies first for caching
COPY Cargo.toml .
COPY Cargo.lock .
RUN --mount=type=cache,target=/root/.cargo/cache \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo fetch --locked

# Copy source code
COPY src ./src
COPY src/**/*.rs ./src

# Build the binary
RUN --mount=type=cache,target=/root/.cargo/cache \
    --mount=type=cache,target=/usr/local/cargo/registry \
    cargo build --release

# Production stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/driver-service /app/

# Expose port
EXPOSE 3001

# Run the binary
CMD ["/app/driver-service"]
