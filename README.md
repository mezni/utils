# BorneMap

EV charging discovery and infrastructure platform for Tunisia.

## Architecture

- **Backend**: Rust + Actix-web v4 + SQLx + PostgreSQL 15 + PostGIS
- **Frontend**: React + Vite + Tailwind CSS
- **Clean Architecture**: presentation → application → domain → infrastructure

## Services

| Service | Port | Auth |
|---------|------|------|
| auth-service | 3001 | None (register/login) |
| admin-service | 3002 | JWT (admin/partner) |
| driver-service | 3003 | Public |

## Quick Start

```bash
docker compose up -d postgres
./scripts/migrate.sh
cargo build --workspace
cargo run -p auth-service
```

## Project Structure

```
bornemap/
├── services/       # Rust microservices
├── crates/         # Shared Rust crates
├── apps/           # React frontends
├── database/       # SQL migrations
├── docs/           # Documentation
└── scripts/        # Dev tooling
```

## Documentation

- [Architecture](docs/architecture.md)
- [Database](docs/database.md)
- [API](docs/api.md)
- [Sprint 01](docs/sprint-01/)
