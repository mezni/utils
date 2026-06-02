FROM rust:1.87 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release -p gis_worker

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/gis_worker /usr/local/bin/gis_worker
EXPOSE 8084
CMD ["gis_worker"]
