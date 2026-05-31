# Service Boundaries

## Backend Services

| Service | Domain | Owns |
|---------|--------|------|
| **driver-service** | Driver | Public station APIs, driver accounts, favorites, reviews |
| **admin-service** | Admin | Station lifecycle, charger lifecycle, partner management, GIS events |
| **clickstream-service** | Analytics | Ingest frontend events, validate payloads, persist to analytics schema |
| **gis-sync-worker** | GIS | Consume outbox, update GIS projections |

## Infrastructure Services

| Service | Purpose |
|---------|---------|
| **postgis** | PostgreSQL + PostGIS database |
| **rabbitmq** | Message broker (AMQP) |
| **keycloak** | Identity and access management |
| **traefik** | Reverse proxy / API gateway |

## Communication

- Service-to-service: Internal Docker network, HTTP
- Asynchronous: RabbitMQ (clickstream events, GIS outbox)
- Database: SQLx connection pool to shared PostgreSQL instance
- Identity: OIDC via Keycloak

## Network

All services communicate over `bornemap-net` Docker bridge network.
Service hostname = container service name in docker-compose.yml.
