# BorneMap Auth Service Dockerfile
FROM rust:1.75-slim-bullseye AS builder

# Set working directory
WORKDIR /app

# Copy Cargo files
COPY auth-service/Cargo.toml auth-service/Cargo.lock auth-service/migrations ./

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy source code
COPY auth-service ./

# Build release binary
RUN cargo build --release --bin auth-service

# Runtime image
FROM debian:bullseye-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/auth-service /app/auth-service

# Create non-root user
RUN useradd -m -u 1000 -s /bin/bash appuser
USER appuser

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/ || exit 1

# Run the binary
CMD ["/app/auth-service"]