FROM rust:1.87 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p driver_service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/driver_service /usr/local/bin/driver_service
EXPOSE 8081
CMD ["driver_service"]
