# Container Architecture

**Phase**: 1 — Foundation
**Related Tasks**: TASK-01 through TASK-58
**Last Updated**: 2026-06-07

---

## C4 Model — Container Diagram (Level 2)

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Host                              │
│                                                                 │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────────────┐   │
│  │  PostgreSQL  │  │   pgAdmin    │  │      Traefik         │   │
│  │  16+PostGIS  │  │  (Dev only)  │  │       v3             │   │
│  │  :5432       │  │  :5050       │  │  :80 / :443          │   │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────────────┘   │
│         │                 │                  │                   │
│         │                 │                  │                   │
│         │          ┌──────▼──────────────────▼───────┐           │
│         │          │        Internal Network          │           │
│         │          │    (Docker bridge / compose)      │           │
│         │          └──────┬──────────────────┬───────┘           │
│         │                 │                  │                   │
│         ▼                 ▼                  ▼                   │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────┐           │
│  │  Driver  │    │    Admin    │    │ Clickstream  │            │
│  │ Service  │    │   Service   │    │   Service    │            │
│  │  Rust    │    │    Rust     │    │    Rust      │            │
│  │  :8080   │    │   :8081     │    │   :8082      │            │
│  └──────────┘    └──────────────┘    └──────────────┘           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘

┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ Driver Web   │    │  Dashboard   │    │ Driver Mobile│
│ React + Vite │    │  React+Vite │    │  Expo SDK 54 │
│ :5173        │    │  :5174      │    │  Device      │
│ Leaflet map  │    │  Sidebar nav│    │  RN Maps     │
└──────────────┘    └──────────────┘    └──────────────┘
       │                   │                   │
       └───────────────────┼───────────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │   Traefik    │
                    │  /api/v1/*      │
                    └──────┬───────┘
                           │
              ┌────────────┼────────────┐
              ▼            ▼            ▼
       Driver Service  Admin Svc  Clickstream Svc
```

## Container Responsibilities

### PostgreSQL 16 + PostGIS 3.4
- Image: `postgis/postgis:16-3.4`
- Port: 5432 (internal)
- Volumes: `postgres_data` for persistence
- Health: `pg_isready -U postgres`
- Schemas: inventory, gis, users, analytics

### Driver Service
- Language: Rust (Actix-web)
- Port: 8080 (internal)
- Image: Built locally from `services/driver-service/Dockerfile`
- Health: `GET /api/v1/health`
- Owns: Public station discovery, driver profile, favorites, reviews
- Reads: inventory, gis
- Writes: users

### Admin Service
- Language: Rust (Actix-web)
- Port: 8081 (internal)
- Image: Built locally from `services/admin-service/Dockerfile`
- Health: `GET /api/v1/health`
- Owns: Partner/Station/Charger CRUD, reporting
- Writes: inventory
- Reads: inventory, users, analytics

### Clickstream Service (Phase 5)
- Language: Rust (Actix-web)
- Port: 8082 (internal)
- Writes: analytics

### Traefik
- Image: `traefik:v3`
- Ports: 80 (HTTP), 443 (HTTPS)
- Routes: `/api/v1/*` to internal services by domain/path
- TLS: Let's Encrypt automatic certificates

### pgAdmin (Development Only)
- Image: `dpage/pgadmin4:latest`
- Port: 5050
- Not included in production Compose file

---

## Docker Compose Configuration

See `infra/compose/docker-compose.yml` for the full configuration.

### Startup Order
1. PostgreSQL starts and becomes healthy
2. Rust services start, run migrations, then accept connections

### Health Check Chain
- PostgreSQL → pg_isready
- Rust services → GET /api/v1/health (validates database connectivity)
- Docker depends_on with `condition: service_healthy`
