FROM rust:1.96-slim-bookworm AS builder

WORKDIR /app
COPY Cargo.toml ./
COPY src/ src/
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/driver-service /usr/local/bin/driver-service

EXPOSE 3001

CMD ["driver-service"]
