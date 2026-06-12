FROM rust:1.88-alpine3.21 AS builder
RUN apk add --no-cache musl-dev pkgconfig openssl-dev protobuf-dev clang lld
ENV RUSTFLAGS="-C link-arg=-fuse-ld=lld" SQLX_OFFLINE=true CARGO_NET_RETRY=3
WORKDIR /app

COPY source/services/ .
RUN cargo build --release --bin admin-service

FROM alpine:3.21 AS runtime
RUN apk add --no-cache ca-certificates tzdata libgcc
RUN addgroup -S app && adduser -S app -G app
ARG APP_PORT=8081
EXPOSE ${APP_PORT}
COPY --from=builder /app/target/release/admin-service /usr/local/bin/service
USER app
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:${APP_PORT}/health || exit 1
ENTRYPOINT ["/usr/local/bin/service"]
