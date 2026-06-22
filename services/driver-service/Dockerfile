FROM rust:slim-bookworm AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin driver-service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/driver-service /usr/local/bin/driver-service
COPY services/driver-service/config.toml /app/config.toml
EXPOSE 3001
WORKDIR /app
CMD ["driver-service"]