# Service Endpoints: Infrastructure Foundation

## Network

All services communicate over the `bornemap-net` Docker bridge network.
Service hostname = container service name in docker-compose.yml.

## Service Contracts

### Traefik

| Property | Value |
|----------|-------|
| Hostname | traefik |
| External Port | 80 (HTTP) |
| Internal Dashboard | 8080 (dev only) |
| Protocol | HTTP |
| Provider | Docker (container labels) |
| Entrypoint | :80 |

**Routes**:
- `/auth/*` → Keycloak (port 8080)

### PostgreSQL + PostGIS

| Property | Value |
|----------|-------|
| Hostname | postgis |
| Internal Port | 5432 |
| Protocol | PostgreSQL wire protocol |
| Database | bornemap |
| User | bornemap |
| Extensions | postgis, uuid-ossp |
| Health Check | `pg_isready -U bornemap` |

**Connection string**: `postgresql://bornemap:bornemap@postgis:5432/bornemap`

### MongoDB

| Property | Value |
|----------|-------|
| Hostname | mongodb |
| Internal Port | 27017 |
| Protocol | MongoDB wire protocol |
| Database | clickstream |
| Auth | None (Phase 1 dev mode) |

**Connection string**: `mongodb://mongodb:27017/clickstream`

### RabbitMQ

| Property | Value |
|----------|-------|
| Hostname | rabbitmq |
| AMQP Port | 5672 |
| Management UI Port | 15672 |
| Protocol | AMQP 0-9-1 |
| Default VHost | / |
| Admin User | admin |
| Management UI | `http://localhost:15672` |

**AMQP connection**: `amqp://admin:admin@rabbitmq:5672/`

### Keycloak

| Property | Value |
|----------|-------|
| Hostname | keycloak |
| Internal Port | 8080 |
| External Route | `/auth/*` via Traefik |
| Protocol | HTTP (REST API + Admin Console) |
| Admin Console | `http://localhost/auth/admin/` |
| Realm | bornemap (auto-imported) |
| Roles | registered_driver, partner, admin |
| DB Backend | PostgreSQL (shared postgis instance) |
