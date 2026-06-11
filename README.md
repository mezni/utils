# BorneMap

EV charging platform — Tunis, Tunisia.

## Architecture

```
source/           ← All runtime code
├── services/     ← Backend microservices + shared libs
├── frontend/     ← Frontend applications
└── packages/     ← Shared workspace packages
infra/            ← Docker Compose, configuration
docs/             ← Architecture, ADRs, specs
scripts/          ← Dev tooling
```

## Quick Start

```bash
# Start infrastructure
./scripts/start.sh

# Stop infrastructure
./scripts/stop.sh

# Check service health
./scripts/healthcheck.sh
```

## Services

| Service | Port | Description |
|---|---|---|
| platform_db | 5432 | System of record (PostgreSQL + PostGIS) |
| analytics_db | 5433 | Event stream (PostgreSQL) |
| Keycloak | 8083 | Identity provider |

## MVP Roadmap

| MVP | Focus |
|---|---|
| MVP-1 | UX Discovery (current) |
| MVP-2 | Admin + Dashboard |
| MVP-3 | Identity + RBAC |
| MVP-4 | Analytics |
| MVP-5 | Performance |
| MVP-6 | Production |

## Docs

See `/docs/constitution.md` for the full system constitution and `/docs/` for architecture, ADRs, and specifications.
