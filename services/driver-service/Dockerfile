# Stage 1: Builder
FROM rust:1.70-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache musl-dev openssl-dev pkg-config

# Copy dependency files
COPY Cargo.toml Cargo.lock ./

# Build dependencies (cached layer)
RUN cargo build --release --bins --all-features

# Copy source code
COPY src ./src

# Build binary
RUN cargo build --release --bins --all-features

# Stage 2: Runner
FROM alpine:3.18

# Install runtime dependencies
RUN apk add --no-cache ca-certificates libgcc

# Copy binary from builder
COPY --from=builder /app/target/release/driver-service /usr/local/bin/driver-service

# Create non-root user
RUN addgroup -g 1000 appuser && \
    adduser -D -u 1000 -G appuser appuser && \
    chown -R appuser:appuser /usr/local/bin

# Switch to non-root user
USER appuser

# Create app directory
WORKDIR /app

# Set environment
ENV POSTGRES_URL=postgres://postgres:postgres@localhost:5432/ev_platform
ENV LOG_LEVEL=info
ENV PORT=8080

# Expose port
EXPOSE 8080

# Run as non-root user
CMD ["driver-service"]
