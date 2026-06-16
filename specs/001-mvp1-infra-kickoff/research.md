# Research: MVP-1 Infra Kickoff

No ambiguities or unknowns to research. All technology choices are specified in existing `docs/spec/*` files. This document confirms the validated decisions.

## Tech Stack Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Service framework | Rust + Actix-web | Specified in sprint-backlog.md, aligned with constitution |
| Mobile app | Expo SDK 54 + react-native-maps | Specified in sprint-backlog.md |
| Web app | React + Leaflet | Specified in architecture-overview.md |
| Dashboard | React + shadcn/ui + React Router | Specified in architecture-overview.md |
| Database | PostGIS 15 (platform_db), Postgres 16 (keycloak_db, analytics_db) | Specified in docker-compose-map.md |
| Cache | Redis 7 | Specified in docker-compose-map.md (MVP-2 scope) |
| Container orchestration | Docker Compose v2 | Specified in docker-compose-map.md |
| Monorepo | pnpm workspaces (TS) + Cargo workspace (Rust) | Standard for mixed TS/Rust monorepos |
| Service port allocation | Auth:3000, Driver:3001, Admin:3002, GIS:3003 | Locked in constitution v1.2 |
| Profile strategy | `infra` profile for databases, `services` for apps | Allows starting DBs without building services |

## Directory Layout

```
borne-map/
├── services/
│   ├── auth-service/
│   ├── driver-service/
│   ├── admin-service/
│   └── gis-service/         # MVP-2
├── apps/
│   ├── mobile-driver/       # Expo
│   ├── web-driver/          # React + Leaflet
│   └── dashboard/           # React + shadcn/ui
├── packages/
│   ├── shared-types/
│   ├── shared-ui/
│   ├── shared-hooks/
│   └── api-client/
├── crates/
│   ├── db-models/
│   └── validation/
├── infra/
│   ├── docker-compose.yml
│   ├── db/
│   │   └── init-platform-db.sql
│   └── osm-importer/        # MVP-2
├── docs/
│   ├── spec/
│   ├── mvp-*/
│   └── adr/
├── specs/                   # Speckit feature specs
├── scripts/
│   └── setup.sh
├── Makefile
├── .env.example
└── AGENTS.md
```
