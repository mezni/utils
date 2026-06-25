# Sprint 07 — Architecture & Implementation Plan

## 1. System Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Docker Compose                      │
│                                                       │
│  ┌─────────────┐    ┌──────────────────────────┐     │
│  │  postgres    │    │        keycloak           │     │
│  │  :5432       │◄───│  quay.io/keycloak:26.0   │     │
│  │              │    │  :8080                    │     │
│  │  Databases:  │    │  Realm: bornemap          │     │
│  │  • bornemap  │    │  Roles: driver, partner,  │     │
│  │  • keycloak_db│   │         admin, super_admin │     │
│  │              │    │  Clients: mobile-driver,   │     │
│  │              │    │           web-driver,       │     │
│  │              │    │           admin-dashboard   │     │
│  └──────┬───────┘    └──────────────────────────┘     │
│         │                                               │
│  ┌──────┴───────┐                                      │
│  │  bornemap    │                                      │
│  │  schemas:    │                                      │
│  │  • gis       │                                      │
│  │  • ev        │                                      │
│  │  • users     │                                      │
│  └─────────────┘                                      │
└─────────────────────────────────────────────────────┘
```

## 2. Affected Modules

| Module | Change | Impact |
|--------|--------|--------|
| `docker-compose.yml` | Add `keycloak` service + `keycloak_db` init | New container |
| `infra/keycloak/` | NEW directory — config, realm, docs | Infrastructure |
| `services/auth-service/` | NEW service scaffold + migrations | Backend scaffold |
| `migrations/init.sh` | Update to source `users/` subdirectory | init script |
| `migrations/platform_db/users/` | NEW directory — users schema migrations | Database |

## 3. Database Impact

### New Database: `keycloak_db`
- Created by init SQL in `docker-entrypoint-initdb.d`
- Managed exclusively by Keycloak
- No application access

### New Schema: `users` (on platform_db)
- Contains `user_profiles` table
- Owned by future `auth-service`
- One-to-one with Keycloak user UUID

## 4. Dependency Graph

```
init.sh (docker-entrypoint)
  ├── gis/*.sql              (existing)
  ├── ev/*.sql               (existing)
  └── users/*.sql            (NEW)
      └── 001_create_users_schema.sql
          └── 002_create_user_profiles.sql

docker-compose.yml
  ├── postgres               (existing, modified: add keycloak_db init)
  └── keycloak               (NEW)
      └── depends_on: postgres
      └── realm: infra/keycloak/realm/bornemap-realm.json
```

## 5. Implementation Order

1. Write sprint documentation (spec → plan → tasks)
2. Create branch
3. Create `migrations/keycloak_db/001_create_keycloak_db.sql`
4. Create `migrations/platform_db/users/` with schema + table SQL
5. Update `migrations/init.sh` to source `users/`
6. Create `infra/keycloak/` with docker-compose, realm, docs
7. Create `services/auth-service/migrations/` (link to platform_db/users)
8. Start Keycloak, import realm, validate
9. Generate delivery artifacts
10. Commit and PR

## 6. Validation Strategy

### Keycloak Validation
```bash
# OIDC discovery
curl http://localhost:8080/realms/bornemap/.well-known/openid-configuration

# JWKS
curl http://localhost:8080/realms/bornemap/protocol/openid-connect/certs

# Token (direct grant)
curl -X POST http://localhost:8080/realms/bornemap/protocol/openid-connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "client_id=admin-dashboard" \
  -d "client_secret=<secret>" \
  -d "grant_type=client_credentials"
```

### Database Validation
```sql
SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'users';
SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'users';
```

## 7. Security Considerations

- Keycloak runs in `start-dev` mode (non-production)
- Client secrets for confidential clients
- PKCE enabled for public clients
- No user data in Keycloak beyond auth
- `user_profiles` stores only profile metadata
