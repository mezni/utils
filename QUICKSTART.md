# BorneMap — Quickstart

## Prerequisites

- Rust 1.85+ (edition 2024)
- Docker Desktop or Docker Compose v2
- Node.js 20+ (for frontend, future sprints)

## Setup

```bash
# 1. Clone and enter
git clone <repo-url>
cd BorneMap

# 2. Copy env
cp .env.example .env

# 3. Build workspace
cargo check

# 4. Start dependencies (PostgreSQL + Redis)
docker compose -f infra/docker-compose.yml up -d postgres redis

# 5. Run auth-service
cargo run -p auth-service
```

## Verify

```bash
curl localhost:8081/health/live   # → 200
curl localhost:8081/health/ready  # → 200
```

## Project Layout

```
Cargo.toml                  # workspace root
shared/
├── bornemap-core/          # domain types, errors, session management
├── bornemap-auth/          # JWT validation, refresh tokens
└── bornemap-db/            # database migrations
services/
├── auth-service/           # auth API with JWT refresh token rotation
├── driver-service/         # driver API (future)
└── admin-service/          # admin API (future)
apps/                       # frontends (future)
infra/
└── docker-compose.yml      # local stack
```

## Commands

| Command | Description |
|---|---|
| `cargo check` | Compile-check entire workspace |
| `cargo build` | Full build |
| `cargo test` | Run all tests |
| `cargo fmt` | Format code |
| `cargo clippy -- -D warnings` | Lint |
| `cargo run -p <service>` | Run a service |
| `docker compose -f infra/docker-compose.yml up` | Start infra stack |

## Services

| Service | Port | Status |
|---|---|---|
| auth-service | 8081 | Sprint 04 - JWT refresh tokens |
| driver-service | — | Planned |
| admin-service | — | Planned |

## Environment

Key env vars (see `.env.example`):

```
HOST=0.0.0.0
PORT=8081
DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres
REDIS_URL=redis://localhost:6379
RUST_LOG=info

# JWT Configuration
JWT_SECRET=your-secret-key-here
JWT_ACCESS_TTL_MINUTES=15
JWT_REFRESH_TTL_DAYS=7
JWT_ISSUER=bornemap
JWT_AUDIENCE=bornemap-app
```
