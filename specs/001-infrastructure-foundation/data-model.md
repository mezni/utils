# Data Model: Infrastructure Foundation (MVP Runtime Core)

No business entities are introduced in Phase 1. This document defines the
configuration model for each infrastructure service — the environment
variables and volumes that establish the runtime.

## Services Configuration Model

### PostgreSQL + PostGIS

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| DB_NAME | string | bornemap | Database name |
| DB_USER | string | bornemap | Database user |
| DB_PASSWORD | string | bornemap | Database password |
| DB_PORT | int | 5432 | Internal container port |
| IMAGE | string | postgis/postgis:16-3.4 | Official PostGIS image |
| VOLUME | named | pg_data | Persistent data volume |
| EXTENSIONS | set | postgis, uuid-ossp | Enabled via init SQL |

**State transitions**: First boot runs init.sql to create extensions.
Data persists across restarts via named volume.

### MongoDB

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| DB_NAME | string | clickstream | Analytics database |
| PORT | int | 27017 | Internal container port |
| IMAGE | string | mongo:7 | Official MongoDB image |
| VOLUME | named | mongo_data | Persistent data volume |
| AUTH | bool | false | Disabled in Phase 1 (dev mode) |

**State transitions**: First boot creates `clickstream` database.
No authentication required in Phase 1.

### RabbitMQ

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| USER | string | admin | Management UI user |
| PASSWORD | string | admin | Management UI password |
| AMQP_PORT | int | 5672 | AMQP protocol port |
| UI_PORT | int | 15672 | Management UI port |
| IMAGE | string | rabbitmq:4-management | Includes management plugin |
| VOLUME | named | rabbitmq_data | Persistent queue data |
| VHOST | string | / | Default virtual host |

**State transitions**: Creates default vhost `/` on first boot.
Queue data persists across restarts.

### Keycloak

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| ADMIN | string | admin | Admin console user |
| ADMIN_PASSWORD | string | admin | Admin console password |
| PORT | int | 8080 | Internal container port |
| IMAGE | string | quay.io/keycloak/keycloak:25 | Official Keycloak image |
| DB_VENDOR | string | postgres | Database backend |
| DB_ADDR | string | postgis | PostgreSQL service name |
| DB_PORT | int | 5432 | PostgreSQL port |
| DB_DATABASE | string | bornemap | Shared database |
| DB_USER | string | bornemap | Database user |
| DB_PASSWORD | string | bornemap | Database password |
| VOLUME | named | keycloak_data | Realm/user persistence |
| REALM_IMPORT | file | /opt/keycloak/data/import/realm.json | Optional realm export |

**State transitions**: First boot creates Keycloak schema in shared
PostgreSQL. Realm auto-import via mounted JSON file. Retries DB
connection on startup.

### Traefik

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| PORT | int | 80 | HTTP entrypoint |
| DASHBOARD_PORT | int | 8080 | Internal dashboard port |
| IMAGE | string | traefik:v3.1 | Official Traefik image |
| PROVIDER | string | docker | Automatic service discovery |
| NETWORK | string | bornemap-net | Single shared network |
| DASHBOARD | bool | true | Enabled in dev mode only |

**State transitions**: Discovers services via Docker socket mount.
Routes based on container labels.

## Network Model

| Network | Driver | Scope |
|---------|--------|-------|
| bornemap-net | bridge | All services |

**Rules**:
- Internal service-to-service communication via container hostname
- No external DB port exposure
- Traefik binds to host port 80 for external access

## Volume Model

| Volume | Driver | Services | Purpose |
|--------|--------|----------|---------|
| pg_data | local | postgis | PostgreSQL data files |
| mongo_data | local | mongodb | MongoDB data files |
| rabbitmq_data | local | rabbitmq | RabbitMQ queue data |
| keycloak_data | local | keycloak | Keycloak realm/user data |
