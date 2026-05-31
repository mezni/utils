# System Overview

## Architecture

BorneMap is a multi-service EV charging station locator for Tunisia.
The system follows a modular monolith architecture with clear domain
boundaries.

```text
[Driver Web]  [Driver Mobile]  [Partner Dashboard]  [Admin Dashboard]
       |              |                 |                    |
       +--------------+-----------------+--------------------+
                              |
                          [Traefik]
                              |
              +---------------+---------------+---------------+
              |               |               |               |
        [Driver-Svc]   [Admin-Svc]  [Clickstream-Svc]  [Keycloak]
              |               |               |               |
              +-------[PostgreSQL]------------+               |
                      |   |   |   |                           |
                  [GIS] [Inv] [Usr] [Analytics]               |
                      |                                       |
                  [GIS Sync Worker]                            |
                      |                                       |
                  [RabbitMQ] ←─────────────────────────────────+
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Rust (Axum, SQLx, Tokio) |
| Frontend (Web) | React, TypeScript, MapLibre/Leaflet |
| Frontend (Mobile) | Expo, React Native |
| Database | PostgreSQL + PostGIS |
| Message Broker | RabbitMQ |
| Identity | Keycloak |
| Proxy | Traefik |
| Deployment | Docker Compose, GitHub Actions, GHCR |

## Key Principles

1. **PostgreSQL-first**: Single database with schema-per-domain
2. **JSONB for analytics**: Flexible schema without MongoDB
3. **Event-driven GIS**: Outbox pattern for spatial projections
4. **Operational simplicity**: Docker Compose, no Kubernetes
