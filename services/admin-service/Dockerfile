FROM rust:latest-slim-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin admin-service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/admin-service /usr/local/bin/admin-service
COPY services/admin-service/config.toml /app/config.toml
EXPOSE 3002
WORKDIR /app
CMD ["admin-service"]