FROM rust:1.96-slim-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY shared shared/
COPY libs/db-core libs/db-core/
COPY libs/geo-core libs/geo-core/
COPY driver-service driver-service/

RUN cargo build --release --package driver-service

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/driver-service /usr/local/bin/driver-service

EXPOSE 3001

CMD ["driver-service"]
