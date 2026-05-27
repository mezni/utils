FROM rust:1.85-slim-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY services/core-service/Cargo.toml services/core-service/
COPY shared/bornemap-types/Cargo.toml shared/bornemap-types/
RUN cargo fetch
COPY . .
RUN cargo build --release -p core-service

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/core-service .
EXPOSE 8080
CMD ["./core-service"]
