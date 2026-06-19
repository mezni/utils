# Keycloak Realm Contract

**File**: `source/infra/keycloak/realm-export/bornemap-realm.json`

## Realm

| Property | Value |
|----------|-------|
| Realm name | `bornemap` |
| Enabled | `true` |
| SSL Required | `none` (dev only, TLS in Sprint 6+) |
| Access Token Lifespan | 300 seconds (5 min) |
| Refresh Token Lifespan | 1800 seconds (30 min) |

## Clients

| Client ID | Client Protocol | Access Type | Direct Access Grants | Service Accounts |
|-----------|----------------|-------------|---------------------|-----------------|
| `mobile-driver-app` | openid-connect | confidential | ✅ | ❌ |
| `web-driver-app` | openid-connect | confidential | ✅ | ❌ |
| `admin-dashboard` | openid-connect | confidential | ✅ | ❌ |

All clients use:
- Standard flow enabled
- Direct access grants enabled (password grant for MVP)
- Valid redirect URIs: `http://localhost:*` (dev)
- Client secret via `KC_CLIENT_SECRET_*` env vars (future)

## Roles

| Role Name | Composite | Included Roles |
|-----------|-----------|---------------|
| `role:admin` | No | — |
| `role:partner` | No | — |
| `role:driver` | No | — |

## Protocol Mappers

Default OIDC mappers plus:

| Name | Claim | Type |
|------|-------|------|
| `client_roles` | `roles` (within token) | User Realm Role |
| `audience` | `aud` | Hardcoded to `admin-dashboard` for admin client |

## Key Endpoints

| Endpoint | Path |
|----------|------|
| Token | `/realms/bornemap/protocol/openid-connect/token` |
| JWKS | `/realms/bornemap/protocol/openid-connect/certs` |
| Admin Console | `/admin/master/console/#/realms/bornemap` |
| Well-known | `/realms/bornemap/.well-known/openid-configuration` |

## Internal Admin User

Keycloak admin console login uses `KEYCLOAK_ADMIN` / `KEYCLOAK_ADMIN_PASSWORD` from `.env`, NOT the Keycloak realm. This is the master realm admin user.
