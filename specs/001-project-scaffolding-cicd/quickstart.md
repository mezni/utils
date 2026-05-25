# BorneMap — Quickstart Guide

## Prerequisites

- Docker & Docker Compose
- Git
- Node.js 20+
- pnpm 9 (`npm install -g pnpm@9`)
- Rust 1.78+ (`rustup install 1.78`)
- `sqlx-cli` (`cargo install sqlx-cli --no-default-features --features postgres`)

## Clone & Setup

```bash
git clone <repo-url> bornemap
cd bornemap
```

## Run the Full Stack

```bash
# Start database + backend API
docker compose -f docker-compose.dev.yml up -d

# Verify backend is healthy
curl http://localhost:8080/api/v1/health

# Run migrations
docker compose exec backend-api sqlx migrate run
```

## Frontend Development

```bash
cd sources/frontend

# Install dependencies
pnpm install

# Start all frontend apps (in separate terminals)
pnpm --filter admin-portal dev
pnpm --filter partner-dashboard dev
pnpm --filter mobile-driver dev
```

## Run Tests

```bash
# Backend tests
cd sources/backend
cargo test

# Frontend tests (once test frameworks are added)
cd sources/frontend
pnpm -r test
```

## CI Pipeline

CI runs automatically on push/PR via GitHub Actions. To run locally:

```bash
# Backend checks
cd sources/backend
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test

# Frontend checks
cd sources/frontend
pnpm -r lint
pnpm -r type-check
pnpm -r build

# Docker smoke test
docker compose -f docker-compose.dev.yml up -d --wait
curl http://localhost:8080/api/v1/health
docker compose -f docker-compose.dev.yml down -v
```

## Common Tasks

```bash
# Add a new migration
cd sources/backend
sqlx migrate add <description>

# Add a new domain module
mkdir -p sources/backend/src/domain/<module_name>
touch sources/backend/src/domain/<module_name>/mod.rs
touch sources/backend/src/domain/<module_name>/repository.rs
touch sources/backend/src/domain/<module_name>/models.rs

# Add a new frontend app
cd sources/frontend
pnpm create vite apps/<app-name> --template react-ts
pnpm install
```
