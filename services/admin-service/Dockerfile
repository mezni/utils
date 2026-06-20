ARG RUST_IMAGE=rust:latest

FROM $RUST_IMAGE AS planner

WORKDIR /app
COPY Cargo.toml Cargo.lock* ./
COPY services/admin-service/Cargo.toml ./services/admin-service/
COPY speckit/speckit-lint/Cargo.toml ./speckit/speckit-lint/

RUN mkdir -p services/admin-service/src services/admin-service/migrations && \
    mkdir -p speckit/speckit-lint/src/rules && \
    echo "fn main() {}" > services/admin-service/src/main.rs && \
    echo "fn main() {}" > speckit/speckit-lint/src/main.rs && \
    SQLX_OFFLINE=true cargo build --release --package admin-service

FROM $RUST_IMAGE AS builder

WORKDIR /app
COPY --from=planner /app/target /app/target
COPY --from=planner /app/services/admin-service/Cargo.toml ./services/admin-service/
COPY --from=planner /app/speckit/speckit-lint/Cargo.toml ./speckit/speckit-lint/
COPY Cargo.toml Cargo.lock* ./

COPY services/admin-service/migrations ./services/admin-service/migrations
COPY services/admin-service/src ./services/admin-service/src

RUN mkdir -p speckit/speckit-lint/src && \
    echo "fn main() {}" > speckit/speckit-lint/src/main.rs && \
    rm -rf /app/target/release/build/admin-service* /app/target/release/.fingerprint/admin-service* /app/target/release/admin-service* /app/target/release/deps/admin-service* && \
    SQLX_OFFLINE=true cargo build --release --package admin-service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/admin-service /usr/local/bin/admin-service
CMD ["admin-service"]
