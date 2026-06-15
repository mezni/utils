# Quickstart: Identity Core (MVP-2)

## What you're building

An authentication and authorization layer for the BorneMap platform using Keycloak as the identity provider. This adds:
- User registration and login for drivers (P1)
- Partner account management by admins (P2)
- Session management (login, logout, refresh) (P1)
- Shared JWT validation for all services (P1)

## Prerequisites

- Existing MVP-1 stack (PostgreSQL 17 + PostGIS, Traefik v3, driver-service)
- Docker and Docker Compose
- Rust toolchain (same as MVP-1, edition 2021)

## New components

| Component | Location | Description |
|-----------|----------|-------------|
| `auth-service` | `source/services/auth-service/` | New Rust service (Actix-Web 4) — handles identity API |
| `identity-core` | `source/services/libs/identity-core/` | Shared Rust crate — JWT validation, Keycloak Admin API client |
| `keycloak` | New Docker container | Identity provider (Quarkus dist) |
| `kcadm scripts` | `source/infra/keycloak/` | Shell scripts for realm/client/role setup |

## New Docker services

| Service | Image | Port | Depends on |
|---------|-------|------|------------|
| keycloak | quay.io/keycloak/keycloak:26+ | 8080 (internal) | postgres |
| auth-service | (custom) | 3000 | postgres, keycloak |

## Database

- New schema: `users` — created by auth-service migrations
- 5 tables: `accounts`, `roles`, `account_roles`, `identity_providers`, `audit_log`
- Migration tool: SQLx migrations (`source/services/auth-service/migrations/`)

## Keycloak configuration

Run `source/infra/keycloak/init-keycloak.sh` to:
1. Create `bm-drivers` realm (public client with PKCE)
2. Create `bm-control` realm (confidential client with service account)
3. Create roles: `registered_driver`, `partner`, `admin`
4. Create seed admin user in `bm-control` realm

## Quick setup

```bash
# 1. Start Keycloak
docker compose up -d keycloak

# 2. Initialize realms/clients/roles
bash source/infra/keycloak/init-keycloak.sh

# 3. Set up the users schema
cd source/services/auth-service && cargo sqlx migrate run

# 4. Start auth-service
cargo run -p auth-service

# 5. Verify
curl localhost/api/v1/health
```

## Environment variables

| Variable | Description | Default |
|----------|-------------|---------|
| `KEYCLOAK_URL` | Keycloak base URL | `http://keycloak:8080` |
| `KEYCLOAK_ADMIN_CLIENT_ID` | Admin API client ID | `bm-admin-cli` |
| `KEYCLOAK_ADMIN_CLIENT_SECRET` | Admin API client secret | (required) |
| `SEED_ADMIN_EMAIL` | Seed admin email | (required) |
| `SEED_ADMIN_PASSWORD` | Seed admin password | (required) |
| `DATABASE_URL` | PostgreSQL connection string | `postgres://...` |
| `HOST` | Auth-service listen address | `0.0.0.0` |
| `PORT` | Auth-service listen port | `3000` |
| `RUST_LOG` | Logging level | `info` |

## Verifying the setup

```bash
# Health check
curl localhost/api/v1/health

# Register a new driver
curl -X POST localhost/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"Test1234!","first_name":"Test","last_name":"User"}'

# Login
curl -X POST localhost/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"Test1234!"}'

# Get current user
curl localhost/api/v1/auth/me \
  -H "Authorization: Bearer <token>"

# Authenticated station discovery
curl localhost/api/v1/stations/nearby?lat=48.8566&lng=2.3522 \
  -H "Authorization: Bearer <token>"
```
