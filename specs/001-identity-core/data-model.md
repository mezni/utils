# Data Model: Identity Core (MVP-2)

**Schema**: `users` (dedicated identity schema, isolated from `inventory`)

---

## Tables

### accounts

Master identity ledger. One row per platform user.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `UUID` | PK, DEFAULT `gen_random_uuid()` | Internal primary key |
| `usr_id` | `VARCHAR(20)` | UNIQUE, NOT NULL | Human-readable identifier: `USR-` + nanoid (12 chars) |
| `keycloak_user_id` | `VARCHAR(36)` | UNIQUE, NOT NULL | Keycloak UUID — immutable once set |
| `email` | `VARCHAR(255)` | UNIQUE, NOT NULL | Login identifier, unique across all realms |
| `email_verified` | `BOOLEAN` | NOT NULL, DEFAULT false | Reserved for post-MVP-2 email verification |
| `first_name` | `VARCHAR(100)` | | |
| `last_name` | `VARCHAR(100)` | | |
| `realm` | `VARCHAR(50)` | NOT NULL | `bm-drivers` or `bm-control` |
| `status` | `VARCHAR(30)` | NOT NULL, DEFAULT 'ACTIVE' | `ACTIVE` or `DISABLED` (PENDING_VERIFICATION deferred) |
| `created_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT `NOW()` | |
| `updated_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT `NOW()` | |
| `disabled_at` | `TIMESTAMPTZ` | | Set when status → DISABLED |

**Indexes**:
- `idx_accounts_email` on `email` (unique)
- `idx_accounts_usr_id` on `usr_id` (unique)
- `idx_accounts_kc_id` on `keycloak_user_id` (unique)
- `idx_accounts_realm` on `realm`
- `idx_accounts_status` on `status`

**Validation rules**:
- `usr_id` MUST match pattern `^USR-[0-9a-zA-Z_-]{12}$`
- `email` MUST be a valid email format
- `realm` MUST be one of `bm-drivers`, `bm-control`
- `status` MUST be one of `ACTIVE`, `DISABLED`

**State transitions**:
```
ACTIVE ──[admin disable]──→ DISABLED
DISABLED ──[admin re-enable]──→ ACTIVE
```

---

### roles

System-defined roles. Managed via seed/migration, not user-facing CRUD in MVP-2.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `UUID` | PK, DEFAULT `gen_random_uuid()` | Internal primary key |
| `rol_id` | `VARCHAR(20)` | UNIQUE, NOT NULL | Human-readable identifier: `ROL-` + nanoid (12 chars) |
| `name` | `VARCHAR(100)` | UNIQUE, NOT NULL | Role identifier: `registered_driver`, `partner`, `admin` |
| `realm` | `VARCHAR(50)` | NOT NULL | Which realm this role belongs to |
| `description` | `VARCHAR(500)` | | |
| `is_system` | `BOOLEAN` | NOT NULL, DEFAULT true | System roles cannot be deleted |
| `created_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT `NOW()` | |

**Seed data**:
| rol_id | name | realm | description |
|--------|------|-------|-------------|
| ROL-... | `registered_driver` | `bm-drivers` | Default role for all driver accounts |
| ROL-... | `partner` | `bm-control` | Partner organization accounts |
| ROL-... | `admin` | `bm-control` | Platform administrators |

---

### account_roles

Many-to-many assignment of roles to accounts.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `account_id` | `UUID` | PK, FK → `accounts.id` ON DELETE CASCADE | |
| `role_id` | `UUID` | PK, FK → `roles.id` ON DELETE RESTRICT | |
| `assigned_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT `NOW()` | |
| `assigned_by` | `UUID` | FK → `accounts.id` | Who assigned the role (null for seed/system assignments) |

**Validation rules**:
- A role MUST be assignable only to accounts in the same realm

---

### identity_providers

Federated identity links (reserved for Google/Apple OIDC in future MVPs).

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `UUID` | PK, DEFAULT `gen_random_uuid()` | |
| `account_id` | `UUID` | FK → `accounts.id` ON DELETE CASCADE | |
| `provider` | `VARCHAR(50)` | NOT NULL | `LOCAL`, `GOOGLE`, `APPLE` |
| `provider_user_id` | `VARCHAR(255)` | | External user ID from the provider |
| `linked_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT `NOW()` | |

**Note**: MVP-2 only uses `LOCAL` provider (email/password via Keycloak). `GOOGLE`/`APPLE` are schema preparation for future MVPs.

---

### audit_log

Immutable audit trail for identity lifecycle events.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `BIGSERIAL` | PK | Monotonically increasing |
| `event_type` | `VARCHAR(50)` | NOT NULL | See event types below |
| `account_id` | `UUID` | FK → `accounts.id` ON DELETE SET NULL | Target account |
| `actor_account_id` | `UUID` | FK → `accounts.id` ON DELETE SET NULL | Who performed the action (null for system events) |
| `ip_address` | `INET` | | Client IP for login/rate-limit events |
| `metadata` | `JSONB` | | Event-specific details (realm, roles, failure reason) |
| `occurred_at` | `TIMESTAMPTZ` | NOT NULL, DEFAULT `NOW()` | |

**Indexes**:
- `idx_audit_event_type` on `event_type`
- `idx_audit_account_id` on `account_id`
- `idx_audit_occurred_at` on `occurred_at`

**Event types**:
| Event | Description |
|-------|-------------|
| `USER_REGISTERED` | New account created |
| `USER_LOGGED_IN` | Successful login |
| `USER_LOGIN_FAILED` | Failed login attempt |
| `USER_LOGGED_OUT` | Explicit logout |
| `ACCOUNT_DISABLED` | Admin disabled account |
| `ACCOUNT_ENABLED` | Admin re-enabled account |
| `ROLE_ASSIGNED` | Role granted to account |
| `ROLE_REVOKED` | Role removed from account |
| `RATE_LIMIT_TRIGGERED` | Rate limit exceeded (IP or account) |

---

## Entity relationship diagram (text)

```
accounts  1──N  account_roles  N──1  roles
accounts  1──N  identity_providers
accounts  1──N  audit_log (as target)
accounts  1──N  audit_log (as actor)
```

## Cross-service contract

All services reference `users.accounts.usr_id` as the foreign key for `created_by`/`updated_by` fields. The `inventory` schema tables use `VARCHAR(20)` for these fields, matching the `USR-` prefix format.

**Example** — driver-service station record:
```sql
ALTER TABLE inventory.stations ADD COLUMN created_by VARCHAR(20) REFERENCES users.accounts(usr_id);
```
