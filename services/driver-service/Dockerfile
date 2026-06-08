# Stage 1: Builder
FROM rust:1.86-alpine AS builder

RUN apk add --no-cache musl-dev openssl-dev pkgconf

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates/ ./crates/
COPY services/driver-service/Cargo.toml ./services/driver-service/
COPY services/admin-service/Cargo.toml ./services/admin-service/

# Create dummy mains for dependency caching
RUN mkdir -p services/driver-service/src && \
    echo "fn main() {}" > services/driver-service/src/main.rs && \
    mkdir -p services/admin-service/src && \
    echo "fn main() {}" > services/admin-service/src/main.rs

RUN cargo build --release --package driver-service

COPY services/driver-service/src ./services/driver-service/src

RUN cargo build --release --package driver-service

# Stage 2: Runner
FROM alpine:3.18

RUN apk add --no-cache ca-certificates libgcc

COPY --from=builder /app/target/release/driver-service /usr/local/bin/driver-service

RUN addgroup -g 1000 appuser && \
    adduser -D -u 1000 -G appuser appuser && \
    chown -R appuser:appuser /usr/local/bin

USER appuser
WORKDIR /app

ENV POSTGRES_URL=postgres://postgres:postgres@localhost:5432/ev_platform
ENV LOG_LEVEL=info
ENV PORT=8080

EXPOSE 8080

CMD ["driver-service"]
