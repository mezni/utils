# Data Model: Identity & Security Core

**Date**: 2026-06-21
**Branch**: `002-identity-security-core`
**Spec**: [spec.md](./spec.md)

---

## Entities

### UserProfile

**Table**: `users.user_profiles` (platform_db)

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| user_id | UUID | PRIMARY KEY | Keycloak UUID (identity dual system) |
| email | VARCHAR(255) | UNIQUE, NOT NULL | User email from Keycloak |
| role | VARCHAR(20) | NOT NULL, CHECK (role IN ('driver','partner','admin')) | Assigned role |
| display_name | VARCHAR(255) | NULL | User display name |
| phone | VARCHAR(50) | NULL | Contact phone |
| locale | VARCHAR(10) | DEFAULT 'en' | User locale preference |
| is_active | BOOLEAN | DEFAULT true | Soft delete flag |
| last_login_at | TIMESTAMPTZ | NULL | Last successful authentication timestamp |
| created_at | TIMESTAMPTZ | DEFAULT NOW() | Record creation timestamp |
| updated_at | TIMESTAMPTZ | DEFAULT NOW() | Last update timestamp |

**Indexes**:
- `idx_user_profiles_email` on (email)
- `idx_user_profiles_role` on (role)

**Identity**: UUID (Keycloak-assigned, per identity dual system rule)

**Notes**:
- Records are created by JIT provisioning on first authentication
- Updated on every authentication to sync role/attributes from Keycloak
- NOT a source of truth — Keycloak is authoritative

---

### AuditEvent

**Table**: `telemetry.raw_events` (analytics_db)

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| event_id | VARCHAR(15) | PRIMARY KEY | Event identifier (EVT-nanoid12) |
| event_type | VARCHAR(100) | NOT NULL | Event type (auth.login_success, auth.login_failure, auth.token_rejected) |
| source_service | VARCHAR(100) | NOT NULL | Originating service (auth-service) |
| event_data | JSONB | NOT NULL | Event payload (see contracts below) |
| idempotency_key | VARCHAR(255) | UNIQUE, NOT NULL | Deduplication key |
| created_at | TIMESTAMPTZ | DEFAULT NOW() | Event timestamp |

**Indexes**:
- `idx_audit_events_type` on (event_type)
- `idx_audit_events_created_at` on (created_at DESC)
- `idx_audit_events_idempotency` on (idempotency_key)

**Identity**: EVT-nanoid(12) per entity identity convention

---

### KeycloakRealm

**Entity**: `bornemap` realm (Keycloak-managed)

| Component | Name | Purpose |
|-----------|------|---------|
| Realm | bornemap | Central identity domain |
| Client | mobile-driver | Driver mobile app OAuth2 client |
| Client | web-driver | Driver web app OAuth2 client |
| Client | admin-dashboard | Admin dashboard OAuth2 client |
| Role | driver | Standard driver access |
| Role | partner | Partner management access |
| Role | admin | Full administrative access |

**Identity**: Managed by Keycloak (not platform_db)

---

## Relationships

```
Keycloak (bornemap realm)
    │
    ├── Authenticates users → issues JWT
    │
    ▼
Traefik (JWT validation via forward-auth)
    │
    ▼
auth-service (JIT provisioning + audit events)
    │                              │
    ├── Reads/Writes              └── Emits audit events via HTTP POST
    │   platform_db.users             to driver-service /api/v1/telemetry/events
    │
    ▼                              ▼
platform_db.user_profiles    driver-service → analytics_db.raw_events
(sync'd projection)             (BUS routing → single-writer preserved)
```

## State Transitions

### Authentication Flow

```
User credentials → Keycloak verifies → JWT issued
    ↓
Traefik validates JWT (forward-auth)
    ↓
Service extracts token → validates signature (JWKS)
    ↓
auth-service JIT: SELECT or INSERT/UPDATE user_profiles
    ↓
auth-service emits: POST auth.login_success to driver-service
    ↓
driver-service writes: INSERT telemetry.raw_events
```

### Role Changes

```
Keycloak admin changes user role
    ↓
User next authenticates → JWT contains new role
    ↓
JIT provisioning UPSERTs user_profiles.role
    ↓
RBAC middleware now enforces new role on subsequent requests
```

## Validation Rules

- **User role**: MUST be one of 'driver', 'partner', 'admin'
- **JWT sub**: MUST be a valid UUID v4
- **Event idempotency_key**: MUST be unique per event to prevent duplicate audit entries
- **Audit event_type**: MUST follow `{service}.{action}` convention (e.g., `auth.login_success`)
- **Token lifetime**: Access tokens ≤ 15 minutes, Refresh tokens ≤ 24 hours
