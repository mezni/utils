FROM rust:1.88-alpine AS builder

# Install build dependencies
RUN apk add --no-cache musl-dev pkgconfig openssl-dev postgresql-dev

WORKDIR /app

# Copy dependencies first for caching
COPY Cargo.toml ./
COPY services/admin-service/Cargo.toml services/admin-service/Cargo.toml
COPY crates ./crates
RUN mkdir -p services/admin-service/src && printf 'fn main() {}\n' > services/admin-service/src/main.rs && cargo build --workspace --release && rm -rf services/admin-service/src

# Copy source code
COPY services/admin-service/src ./services/admin-service/src
COPY services/admin-service/migrations ./services/admin-service/migrations

# Build the application
RUN cargo build -p admin-service --release

# Production image
FROM alpine:latest

RUN apk add --no-cache ca-certificates openssl postgresql-libs

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/admin-service /app/admin-service

# Copy environment files
COPY services/admin-service/.env ./.

# Expose port
EXPOSE 8080

# Run the application
CMD ["/app/admin-service"]
