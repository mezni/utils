FROM rust:slim-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin auth-service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/auth-service /usr/local/bin/auth-service
COPY services/auth-service/config.toml /app/config.toml
EXPOSE 3000
WORKDIR /app
CMD ["auth-service"]