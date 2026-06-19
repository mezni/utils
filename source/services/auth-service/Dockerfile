# Build stage
FROM rust:1.83-slim-bullseye AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests and lock file
COPY Cargo.toml Cargo.lock ./

# Download dependencies
RUN cargo fetch --locked

# Copy source code
COPY src ./src

# Build release binary
RUN cargo build --release --bin auth-service

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 -s /bin/bash auth && \
    mkdir -p /app /var/log/auth-service && \
    chown -R auth:auth /app /var/log/auth-service

# Copy binary
COPY --from=builder /app/target/release/auth-service /app/

# Switch to non-root user
USER auth

# Create logs directory
RUN mkdir -p /var/log/auth-service

# Environment variables
ENV RUST_LOG=auth_service=info \
    DATABASE_URL=postgresql://localhost:5432/platform_db \
    PORT=3000

# Expose port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:3000/health || exit 1

# Run application
ENTRYPOINT ["/app/auth-service"]
