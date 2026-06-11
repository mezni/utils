# Quickstart: Clickstream Service

**Phase**: Phase 1 | **Date**: 2026-06-11 | **Feature**: [spec.md](spec.md)

## Prerequisites

- Rust 1.96+
- PostgreSQL 16+ (analytics_db on port 5433)
- Docker (optional, for running analytics_db locally)

## Setup

### 1. Start analytics_db

```bash
docker run -d \
  --name borne-analytics \
  -e POSTGRES_USER=borne \
  -e POSTGRES_PASSWORD=borne \
  -e POSTGRES_DB=analytics \
  -p 5433:5432 \
  postgres:16-alpine
```

### 2. Set environment variables

```bash
export DATABASE_URL_ANALYTICS="postgres://borne:borne@localhost:5433/analytics"
export RUST_LOG="info,clickstream_service=debug"
export CLICKSTREAM_BIND_ADDR="0.0.0.0:8082"
export RATE_LIMIT_BURST_SIZE=100
```

### 3. Run migrations

Migrations run automatically on startup via sqlx embedded migrations. To run manually:

```bash
# From source/services/clickstream-service/
cargo run --bin clickstream-migrate
```

### 4. Build & run

```bash
# From repo root (source/)
cargo build -p clickstream-service

# Run
cargo run -p clickstream-service
```

### 5. Verify

```bash
# Health check
curl http://localhost:8082/api/v1/health

# Ingest an event
curl -X POST http://localhost:8082/api/v1/events \
  -H "Content-Type: application/json" \
  -d '{"event_name":"map_open","user_id":"usr_1","session_id":"sess_1","client_ts":"2026-06-11T12:00:00Z"}'

# Ingest a batch
curl -X POST http://localhost:8082/api/v1/events/batch \
  -H "Content-Type: application/json" \
  -d '[{"event_name":"station_view","user_id":"usr_1","session_id":"sess_1","client_ts":"2026-06-11T12:00:01Z","payload":{"station_id":"st_42"}}]'
```

## Testing

```bash
# Unit tests
cargo test -p clickstream-service --lib

# Integration tests (requires running analytics_db)
cargo test -p clickstream-service --test integration
```

## Project Structure

```
source/services/clickstream-service/
├── Cargo.toml
├── src/
│   ├── main.rs
│   ├── routes/
│   │   ├── mod.rs
│   │   ├── ingest.rs
│   │   └── health.rs
│   ├── models/
│   │   ├── mod.rs
│   │   └── event.rs
│   ├── db/
│   │   ├── mod.rs
│   │   └── repository.rs
│   ├── middleware/
│   │   ├── mod.rs
│   │   └── rate_limiter.rs
│   ├── errors.rs
│   └── response.rs
└── migrations/
    └── 001_create_raw_events.sql
```

## Configuration

| Env Variable | Default | Description |
|-------------|---------|-------------|
| `DATABASE_URL_ANALYTICS` | `postgres://borne:borne@localhost:5432/analytics` | analytics_db connection string |
| `CLICKSTREAM_BIND_ADDR` | `0.0.0.0:8082` | Bind address for the HTTP server |
| `RUST_LOG` | `info` | Log level filter |
| `RATE_LIMIT_BURST_SIZE` | `100` | Max requests per IP per burst window |

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/v1/events` | Ingest single event |
| POST | `/api/v1/events/batch` | Ingest batch of events |
| GET | `/api/v1/health` | Health check |

See [contracts/api.md](contracts/api.md) for full request/response schemas.
