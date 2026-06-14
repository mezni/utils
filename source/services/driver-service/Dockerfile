# Stage 1: Builder
FROM rust:latest as builder

WORKDIR /app

# Copy workspace files
COPY source/services/ /app/

# Build the driver-service in release mode
RUN cargo build --release --bin driver-service

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/driver-service /usr/local/bin/

EXPOSE 8081

CMD ["driver-service"]
