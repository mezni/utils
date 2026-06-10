# Quickstart: Keycloak Setup

**Date**: 2026-06-10 | **Branch**: `013-keycloak-setup`

## Prerequisites

- Docker Engine 24+ with Docker Compose plugin
- All existing MVP-2 services running (postgres, admin-service, driver-service, dashboard, driver-web, driver-mobile)
- Port 8180 available on host

## Setup Steps

### 1. Add Database Schema Migration

```bash
# Migration 0005_keycloak_schema.sql already created
# It runs automatically as part of the standard migration pipeline
```

### 2. Configure Environment Variables

```bash
# Copy and edit Keycloak env vars
cp infra/env/keycloak.env.example .env
# Edit .env with actual values (admin password, Google/Facebook credentials)
```

### 3. Start Keycloak

```bash
# From repo root
docker compose up -d keycloak
```

### 4. Verify Health

```bash
# Wait for health check (may take 60-120s on first run)
docker compose ps keycloak --filter health=healthy

# Check realm endpoint
curl http://localhost:8180/realms/ev-platform
```

### 5. Configure Realm (First Run Only)

1. Open `http://localhost:8180` in browser
2. Log in with `KEYCLOAK_ADMIN` / `KEYCLOAK_ADMIN_PASSWORD`
3. Create realm: `ev-platform`
4. Create roles: `registered_driver` (default), `partner`, `admin`
5. Create clients and configure as specified in data model
6. Configure Google and Facebook IdPs
7. Add `partner_id` protocol mapper to partner client scope

### 6. Export Realm

```bash
docker exec ev-keycloak \
  /opt/keycloak/bin/kc.sh export \
  --realm ev-platform \
  --users realm_file \
  --file /tmp/realm-export.json

docker cp ev-keycloak:/tmp/realm-export.json \
  infra/keycloak/realm-export.json
```

### 7. Verify Clean Import

```bash
docker compose down -v
docker compose up keycloak -d
# Wait for healthy
curl http://localhost:8180/realms/ev-platform
# Must return realm metadata JSON, not 404
docker compose up -d admin-service driver-service dashboard driver-web driver-mobile
# Verify all MVP-2 services pass health checks
```

## Testing

### Email/Password Registration

```bash
# Use Keycloak REST API or admin console to register a user
# Verify JWT contains registered_driver role
```

### Google SSO Login

```bash
# Navigate to login page, select "Sign in with Google"
# Complete authorization, verify JWT is returned
```

### Token Validation

```bash
# Decode JWT payload (base64 decode the second segment)
# Verify claims: sub, email, realm_access.roles, exp, iat, iss
```

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| Health check fails | PostgreSQL not ready | Wait for postgres health check |
| Realm not found (404) | First run, realm not configured | Set up realm via admin console |
| Cannot login to admin console | Wrong credentials | Check `KEYCLOAK_ADMIN_PASSWORD` in `.env` |
| Social login fails | Invalid IdP credentials | Verify Google/Facebook dev credentials |
| `partner_id` missing in JWT | User attribute not set | Set `partner_id` in admin console user attributes |
