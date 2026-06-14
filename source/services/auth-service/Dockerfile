# Stage 1: Builder
FROM rust:latest as builder

WORKDIR /app

# Copy workspace files
COPY source/services/ /app/

# Build the auth-service in release mode
RUN cargo build --release --bin auth-service

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/auth-service /usr/local/bin/

EXPOSE 3000

CMD ["auth-service"]
