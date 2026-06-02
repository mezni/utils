FROM rust:1.87 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p admin_service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/admin_service /usr/local/bin/admin_service
EXPOSE 8082
CMD ["admin_service"]
