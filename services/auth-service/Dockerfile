FROM rust:1.96 as builder

WORKDIR /app
COPY . .

RUN cargo build --release

FROM debian:bookworm-slim

WORKDIR /app
COPY --from=builder /app/target/release/auth-service .

CMD ["./auth-service"]
