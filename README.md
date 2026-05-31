# BorneMap

EV charging station locator for Tunisia.

## Quick Start

```bash
cp infrastructure/env/.env.example .env
make up
```

## Structure

```
apps/             Frontend applications (React, Expo)
services/         Backend services (Rust/Axum)
packages/         Shared libraries
database/         Migrations and schemas
infrastructure/   Docker, deployment, config
docs/             Architecture, specs, decisions
```

## Stack

- **Backend**: Rust (Axum, SQLx, Tokio)
- **Frontend Web**: React, TypeScript, MapLibre
- **Frontend Mobile**: Expo, React Native
- **Database**: PostgreSQL + PostGIS (JSONB for analytics)
- **Queue**: RabbitMQ
- **Auth**: Keycloak
- **Proxy**: Traefik
