FROM rust:1.80-alpine3.20 AS chef
RUN apk add --no-cache musl-dev pkg-config openssl-dev protoc clang lld
ENV RUSTFLAGS="-C link-arg=-fuse-ld=lld"
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG SERVICE_NAME=driver-service
ENV SQLX_OFFLINE=true
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json --bin ${SERVICE_NAME}
COPY . .
RUN cargo build --release --bin ${SERVICE_NAME}

FROM alpine:3.20 AS runtime
RUN apk add --no-cache ca-certificates tzdata libgcc
RUN addgroup -S app && adduser -S app -G app
ARG SERVICE_NAME=driver-service
ARG APP_PORT=8080
EXPOSE ${APP_PORT}
COPY --from=builder /app/target/release/${SERVICE_NAME} /usr/local/bin/service
USER app
HEALTHCHECK --interval=10s --timeout=3s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:${APP_PORT}/health || exit 1
ENTRYPOINT ["/usr/local/bin/service"]
