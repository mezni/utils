FROM rust:1.87 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p analytics_writer

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/analytics_writer /usr/local/bin/analytics_writer
EXPOSE 8080
CMD ["analytics_writer"]
