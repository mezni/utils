# BorneMap

EV charging station discovery and management platform for the Tunisian market.

**Constitution**: v1.15.2 | **Status**: Bootstrap | **Architecture**: 3-service topology

---

## Architecture

```
auth-service :3000  →  users schema (identity projection)
driver-service :3001 → gis schema + analytics_db (GIS + telemetry)
admin-service :3002  → inventory schema (admin orchestration)
```

- **3 services exactly** — immutable topology
- **Strict ownership** — single writer per schema
- **UUID for users, nanoid for entities** — never mixed
- **SQLx compile-time** — no runtime SQL
- **Analytics single-writer** — driver-service only

## Repository Structure

```
bornemap/
  apps/
    mobile-driver/        # Expo SDK 54
    web-driver/           # React + Leaflet
    admin-dashboard/      # React + shadcn/ui
    packages/
      ui-kit/             # UI ONLY (components, tokens, layouts)
      domain-types/       # Contracts ONLY (DTOs, schemas, types)
      client-core/        # Transport ONLY (API clients, auth)
  backend/
    auth-service/         # :3000
    driver-service/       # :3001
    admin-service/        # :3002
    shared/
      shared-domain/      # Pure types only
      shared-infra/       # Infra utilities only
  infrastructure/
    docker-compose/
    keycloak/
    traefik/
    scripts/
  tools/                  # CI enforcement scripts
  docs/                   # Constitution, architecture, sprints
```

## Getting Started

TBD — prerequisites and setup instructions.

## Documentation

| Document | Description |
|----------|-------------|
| [Constitution](docs/constitution/constitution.md) | Architecture rules and invariants |
| [SpecKit Enforcement](docs/constitution/speckit_enforcement.md) | CI enforcement layer |
| [Guardrails](docs/constitution/guardrails.md) | Operational boundaries |
| [Architecture](docs/architecture.md) | System architecture overview |
| [System State](docs/SYSTEM_STATE.md) | Current system status |
| [Roadmap](docs/roadmap_status.md) | Sprint pipeline and milestones |

## Sprints

| Sprint | Title | Status |
|--------|-------|--------|
| 0 | System Bootstrap & Enforcement Kernel | IN PROGRESS |
| 1 | Identity & Security Core | NOT_STARTED |
| 2 | GIS Engine Foundation | NOT_STARTED |
| 3 | Inventory System | NOT_STARTED |
| 4 | Telemetry Ingestion Core | NOT_STARTED |
| 5 | Analytics Read Layer | NOT_STARTED |
| 6 | Driver Experience Layer | NOT_STARTED |
| 7 | System Hardening & Reliability | NOT_STARTED |
| 8 | Security Hardening & Compliance | NOT_STARTED |
| 9 | Production Release & Operations | NOT_STARTED |

## License

TBD
