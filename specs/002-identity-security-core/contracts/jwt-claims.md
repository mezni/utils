# JWT Claims Contract

**Purpose**: Define the structure of JWT payload claims issued by Keycloak.

## Standard Claims

| Claim | Type | Required | Description |
|-------|------|----------|-------------|
| sub | UUID v4 | yes | Keycloak user UUID (maps to user_profiles.user_id) |
| iss | String | yes | Token issuer (Keycloak realm URL) |
| aud | String | yes | Audience (client ID) |
| exp | i64 | yes | Expiration timestamp (seconds since epoch) |
| iat | i64 | yes | Issued-at timestamp (seconds since epoch) |
| email | String | no | User email address |
| preferred_username | String | no | Username |

## Custom Claims

| Claim | Type | Required | Description |
|-------|------|----------|-------------|
| realm_access.roles | String[] | yes | Keycloak realm roles assigned to user |
| bornemap_role | String | no | Mapped BorneMap role (driver, partner, admin) |

## Role Mapping

The `realm_access.roles` array contains Keycloak realm roles. The first role that matches a known BorneMap role is extracted as the effective role.

```
Keycloak role → BorneMap role
driver        → driver
partner       → partner
admin         → admin
```

## Validation Rules

- `sub` MUST be a valid UUID v4 format
- `exp` MUST be in the future (within configured clock skew)
- `iss` MUST match the configured Keycloak realm URL
- `aud` MUST include the service's client ID
- Token MUST be signed with a valid Keycloak JWKS key
- Token MUST NOT be expired

## Domain-Types Representation

```rust
// In domain-types crate — NOT implementation
pub struct JwtClaims {
    pub sub: Uuid,
    pub email: Option<String>,
    pub role: Role,
    pub exp: i64,
    pub iat: i64,
}
```
