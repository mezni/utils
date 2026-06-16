# BorneMap — Quickstart

## Prerequisites

- Docker Engine 24+ with Compose v2
- Node.js 20+ with pnpm 9+
- Rust 1.80+ with Cargo
- Expo CLI (`npm install -g expo-cli`)

## Setup

```bash
# Clone and enter project
git clone <repo-url> borne-map
cd borne-map

# First-time setup
make setup          # copies .env.example → .env, creates directories

# Start infrastructure (databases)
make up             # starts platform_db, keycloak_db, analytics_db

# Build and start all services
make up-all         # includes auth-service, driver-service, admin-service
```

## Verify

```bash
# Health checks
curl localhost:3000/api/v1/health      # {"status":"ok","service":"auth-service","version":"0.1.0"}
curl localhost:3001/api/v1/health      # {"status":"ok","service":"driver-service","version":"0.1.0"}
curl localhost:3002/api/v1/health      # {"status":"ok","service":"admin-service","version":"0.1.0"}

# Readiness checks
curl localhost:3000/api/v1/health/ready # {"status":"ready"}
```

## Client Apps

```bash
# Mobile driver app
cd apps/mobile-driver && npx expo start

# Web driver app
cd apps/web-driver && pnpm dev

# Dashboard
cd apps/dashboard && pnpm dev
```

## Project Structure

```
borne-map/
├── services/        # Rust backend services (auth, driver, admin, gis)
├── apps/            # Client apps (mobile-driver, web-driver, dashboard)
├── packages/        # Shared TS packages (types, ui, hooks, api-client)
├── crates/          # Shared Rust crates (db-models, validation)
├── infra/           # Docker Compose, DB init scripts
├── docs/            # Specifications, MVP status
└── specs/           # Speckit feature specs
```

## Useful Commands

```bash
make logs SERVICE=auth-service   # Tail service logs
make db-shell                    # Interactive psql into any database
make down                        # Stop all containers
make clean                       # Remove containers + volumes (destructive)
```
