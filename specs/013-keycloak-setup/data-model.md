# Data Model: Keycloak Realm Configuration

**Date**: 2026-06-10 | **Branch**: `013-keycloak-setup` | **Spec**: [spec.md](./spec.md)

## Realm

| Attribute | Value |
|-----------|-------|
| Name | `ev-platform` |
| Display name | EV Platform |
| Access token lifespan | 15 minutes |
| Refresh token lifespan | 7 days |
| SSO session max | 7 days |
| Default role | `registered_driver` |
| Login theme | Base (English only for Sprint 3.1) |
| Account theme | Base |

## Roles

| Role | Description | Assignment |
|------|-------------|------------|
| `registered_driver` | Default role for all registered users | Automatic on registration / first broker login |
| `partner` | Partner organization user | Manual (admin console) |
| `admin` | Platform administrator | Manual (admin console) |

## Clients

### Public Clients (PKCE S256 Required)

| Client | Redirect URIs | Web Origins | Auth Flow |
|--------|---------------|-------------|-----------|
| `driver-web` | `http://localhost:5173/*`, `https://evplatform.tn/*` | `+` (same as redirects) | Authorization Code + PKCE |
| `driver-mobile` | `ev-platform://auth/callback`, `exp://localhost:8081/*` | — | Authorization Code + PKCE |
| `dashboard` | `http://localhost:5174/*`, `https://dashboard.evplatform.tn/*` | `+` | Authorization Code + PKCE |

### Confidential Clients

| Client | Auth Method | Service Account |
|--------|-------------|-----------------|
| `driver-service` | Client ID + Secret | Enabled |
| `admin-service` | Client ID + Secret | Enabled |

## Identity Providers

| Provider | Client ID Source | First Login Flow | Default Scopes | Post-Login Role |
|----------|-----------------|------------------|----------------|-----------------|
| Google | Google Cloud Console | `first broker login` | `openid email profile` | `registered_driver` |
| Facebook | Meta Developer Portal | `first broker login` | `openid email profile` | `registered_driver` |

## Protocol Mappers

### partner_id Mapper (for partner client scope)

| Attribute | Value |
|-----------|-------|
| Name | `partner_id_mapper` |
| Mapper type | User Attribute |
| User attribute | `partner_id` |
| Token claim name | `partner_id` |
| Claim JSON type | String |
| Add to access token | `true` |
| Add to userinfo | `true` |

## JWT Token Structure

```json
{
  "sub": "uuid-of-user",
  "email": "user@example.com",
  "email_verified": true,
  "realm_access": {
    "roles": ["registered_driver"]
  },
  "partner_id": "PRT-00123",
  "exp": 1718000000,
  "iat": 1717999100,
  "iss": "http://keycloak:8180/realms/ev-platform"
}
```

Notes:
- `partner_id` claim is present only when the user has the `partner` role AND the `partner_id` user attribute is set
- `realm_access.roles` always includes `registered_driver` (the default role)
- Token is signed with RS256; JWKS endpoint at `/realms/ev-platform/protocol/openid-connect/certs`

## Database Schema

Only one migration needed:

```sql
-- database/migrations/0005_keycloak_schema.sql
CREATE SCHEMA IF NOT EXISTS keycloak;
```

Keycloak manages all its tables within this schema automatically.
