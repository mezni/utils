# Data Model: Auth Service — User Profile

**Database**: `platform_db`
**Schema**: `users`
**Owner**: `auth_service_role`

## Entity: User Profile

Represents an authenticated user in the platform. Created and updated exclusively by the Auth Service on each login or token refresh. Keyed to the Keycloak `sub` claim.

| Column | Type | Constraints | Description |
|--------|------|-------------|-------------|
| `id` | `TEXT` | `PRIMARY KEY CHECK (id ~ '^USR-.+')` | NanoID with USR- prefix, derived from Keycloak `sub` claim |
| `keycloak_sub` | `TEXT` | `NOT NULL UNIQUE` | Keycloak user identifier (the `sub` claim) |
| `email` | `VARCHAR(255)` | | Email address from Keycloak profile |
| `display_name` | `VARCHAR(255)` | | Display name from Keycloak profile |
| `roles` | `TEXT[]` | `NOT NULL DEFAULT '{}'` | Realm roles from Keycloak (`role:admin`, `role:partner`, `role:driver`) |
| `last_login_at` | `TIMESTAMPTZ` | `NOT NULL DEFAULT NOW()` | Last authentication timestamp |
| `created_at` | `TIMESTAMPTZ` | `NOT NULL DEFAULT NOW()` | First profile creation |
| `updated_at` | `TIMESTAMPTZ` | `NOT NULL DEFAULT NOW()` | Last profile update |

### Indexes

```sql
CREATE UNIQUE INDEX idx_users_keycloak_sub ON users.users (keycloak_sub);
```

### Trigger

```sql
CREATE TRIGGER set_updated_at
    BEFORE UPDATE ON users.users
    FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();
```

### State Transitions

- **Create**: First successful login by a new Keycloak user
- **Update**: Subsequent logins or token refreshes (email, display_name, roles, last_login_at)
- **No soft delete**: User profiles are never soft-deleted per constitution (Section I.5)

### Relationships

- Auth Service is the sole reader and writer. No other service accesses the `users` schema.
- Downstream services (Admin, Driver) identify users via `X-User-Id` header injected by Traefik (not by querying the `users` table).
