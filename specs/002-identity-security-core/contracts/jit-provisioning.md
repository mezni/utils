# JIT Provisioning Contract

**Purpose**: Define the contract for just-in-time user profile provisioning on first authentication.

## Flow

```
1. User authenticates via Keycloak → receives JWT
2. auth-service JWT middleware validates token
3. auth-service JIT handler extracts user info from JWT claims
4. JIT handler attempts SELECT user_profiles WHERE user_id = sub
5. If found: UPDATE role, email, last_login_at, updated_at
6. If not found: INSERT INTO user_profiles (user_id, email, role, ...)
7. Return user profile to caller
```

## SQL Contract

```sql
-- Upsert pattern used by JIT provisioning
INSERT INTO users.user_profiles (user_id, email, role, display_name, last_login_at)
VALUES ($1, $2, $3, $4, NOW())
ON CONFLICT (user_id) DO UPDATE SET
    email = EXCLUDED.email,
    role = EXCLUDED.role,
    display_name = COALESCE(EXCLUDED.display_name, users.user_profiles.display_name),
    last_login_at = NOW(),
    updated_at = NOW()
RETURNING *;
```

## Error Handling

| Condition | Behavior |
|-----------|----------|
| Keycloak unreachable | Return 503, log to local buffer, retry on next request |
| JWT invalid/expired | Return 401, no provisioning attempted |
| Database connection error | Return 500, no user state created |
| Unknown role in JWT | Default to 'driver', log warning |

## Domain-Types Representation

```rust
pub struct UserProfile {
    pub user_id: Uuid,
    pub email: String,
    pub role: Role,
    pub display_name: Option<String>,
    pub is_active: bool,
}

pub enum Role {
    Driver,
    Partner,
    Admin,
}
```
