FROM rust:1.87 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p clickstream_service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/clickstream_service /usr/local/bin/clickstream_service
EXPOSE 8083
CMD ["clickstream_service"]
