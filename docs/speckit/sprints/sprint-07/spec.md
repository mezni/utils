# Sprint 07 — Keycloak Installation + Users Schema Foundation

**Status**: SPEC WRITTEN
**Date**: 2026-06-25
**Constitution Version**: v1.15.2

---

## Scope Lock (HARD CONSTRAINT)

| Domain | Included | Excluded |
|--------|----------|----------|
| **Infrastructure** | Keycloak deployment, realm import, roles, clients | No application integration |
| **Database** | `users` schema, `user_profiles` table | No business logic |
| **Auth** | Keycloak + keycloak_db | No auth-service business logic |
| **Security** | OIDC/JWT validation | No RBAC enforcement |
| **Frontend** | ❌ None | No login UI |
| **Event Bus** | ❌ None | No event bus |
| **Analytics** | ❌ None | No changes |

## Objective

Establish the platform identity foundation:
1. Install and configure Keycloak with dedicated `keycloak_db`
2. Create `platform_db.users` schema with `user_profiles` table
3. Create BorneMap realm with roles and clients
4. Validate OIDC/JWT issuance

## Architecture After Sprint

```
Keycloak
    │
    ▼
keycloak_db

platform_db
 ├── users
 │    └── user_profiles
 ├── gis
 ├── ev
 └── inventory
```

## Deliverables

### Infrastructure (`infra/keycloak/`)
- `docker-compose.keycloak.yml` — standalone Keycloak deployment
- `realm/bornemap-realm.json` — realm import
- `README.md` — Keycloak operations guide
- `validation.md` — validation procedures

### Database Migrations (`services/auth-service/migrations/`)
- `0001_create_users_schema.sql` — `CREATE SCHEMA IF NOT EXISTS users`
- `0002_create_user_profiles.sql` — `CREATE TABLE users.user_profiles`

### Docker Entrypoint Init
- `migrations/keycloak_db/001_create_keycloak_db.sql` — creates `keycloak_db` on postgres

## Database Tasks

### Keycloak Database
- Name: `keycloak_db`
- Owner: Keycloak only
- No application tables, no business logic, no service writes

### Users Schema (`platform_db.users`)
```sql
CREATE SCHEMA IF NOT EXISTS users;
```

### User Profiles Table
```sql
CREATE TABLE users.user_profiles (
    user_uuid UUID PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    phone VARCHAR(50),
    locale VARCHAR(20),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ NULL
);
```

#### Constitution Compliance
| Rule | Status |
|------|--------|
| §10.3 Human identity = Keycloak UUID only | ✅ `user_uuid` PK = Keycloak `sub` |
| §10.1 No business entities in auth | ✅ User profiles only |

## Keycloak Configuration

### Container
- Name: `keycloak`
- Image: `quay.io/keycloak/keycloak:26.0`
- Mode: `start-dev`
- Port: `8080`
- Database: `keycloak_db` on `postgres` service
- Health: `/health`

### Realm: `bornemap`

### Roles
| Role | Description |
|------|-------------|
| `driver` | Mobile app user (EV driver) |
| `partner` | Partner/admin user |
| `admin` | Platform administrator |
| `super_admin` | Super administrator |

### Clients
| Client | Type | Auth | Notes |
|--------|------|------|-------|
| `mobile-driver` | Public | PKCE | Mobile app |
| `web-driver` | Public | PKCE | Web map app |
| `admin-dashboard` | Confidential | Client Secret | Admin panel |

### JWT Claims
```json
{
  "sub": "uuid",
  "email": "user@example.com",
  "realm_access": { "roles": ["driver"] }
}
```

## Acceptance Criteria

### Keycloak
- [ ] Keycloak container running
- [ ] keycloak_db created
- [ ] bornemap realm imported
- [ ] Roles created
- [ ] Clients created
- [ ] OIDC discovery endpoint reachable
- [ ] JWKS endpoint reachable
- [ ] Access token issued successfully

### Database
- [ ] `users` schema exists
- [ ] `user_profiles` table exists
- [ ] `user_uuid` is UUID PK
- [ ] `email` unique constraint exists
- [ ] `deleted_at` column exists

## Constitution Validation

| Rule | Status |
|------|--------|
| §2.1 Exactly 3 services maintained | ✅ No new service |
| §10.3 UUID only for users | ✅ |
| §10.1 No business entities in Keycloak | ✅ |
| Ownership: auth-service → users schema | ✅ |
| No additional database beyond keycloak_db | ✅ |
