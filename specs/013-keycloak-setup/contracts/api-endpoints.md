# Keycloak API Endpoints

## Overview

Key REST API endpoints used during development, testing, and realm administration.

## Admin Console

| Attribute | Value |
|-----------|-------|
| URL | `http://localhost:8180` |
| Auth | `KEYCLOAK_ADMIN` / `KEYCLOAK_ADMIN_PASSWORD` |

## Realm Endpoints

| Endpoint | Purpose | Method |
|----------|---------|--------|
| `/realms/ev-platform` | Realm metadata (health check) | GET |
| `/realms/ev-platform/.well-known/openid-configuration` | OIDC discovery document | GET |
| `/realms/ev-platform/protocol/openid-connect/token` | Token endpoint (login, service auth) | POST |
| `/realms/ev-platform/protocol/openid-connect/auth` | Authorization endpoint (user login) | GET |
| `/realms/ev-platform/protocol/openid-connect/certs` | JWKS public keys | GET |
| `/realms/ev-platform/protocol/openid-connect/userinfo` | User info endpoint | GET |
| `/realms/ev-platform/protocol/openid-connect/logout` | Logout | POST |

## Health Check

```bash
curl -f http://localhost:8180/realms/ev-platform
```

Returns realm metadata JSON on success, 404 on missing realm (first run before configuration).

## Export / Import

### Export

```bash
docker exec ev-keycloak \
  /opt/keycloak/bin/kc.sh export \
  --realm ev-platform \
  --users realm_file \
  --file /tmp/realm-export.json

docker cp ev-keycloak:/tmp/realm-export.json \
  infra/keycloak/realm-export.json
```

### Import (automatic)

Mount `infra/keycloak/realm-export.json` to `/opt/keycloak/data/import/realm.json` and start with `--import-realm`.
