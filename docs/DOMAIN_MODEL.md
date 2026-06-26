# BorneMap Domain Model

Version: v1  
Status: Draft  
Updated: 2026-06-26  

---

## 1 · Naming Conventions

- Domain types live in `bornemap-core` (shared) or per-service `domain/` module
- Enums are `PascalCase`, fields are `snake_case`
- Domain types are pure Rust — no serde, sqlx, actix, tokio
- Errors use `thiserror`

---

## 2 · Auth Domain (auth-service)

### Entities

#### User

```
User {
    id: Uuid
    email: EmailAddress        // value object, case-insensitive
    password_hash: Option<PasswordHash>
    role: UserRole
    status: UserStatus
    email_verified: bool
    created_at: DateTime<Utc>
}
```

Rules:
- At least one of `password_hash` or `oauth_accounts` must exist
- Email is case-insensitive (always stored/lowered)
- `DELETED` users retain their ID for audit trail

#### OAuthAccount

```
OAuthAccount {
    id: Uuid
    user_id: Uuid
    provider: OAuthProvider
    provider_user_id: String
    email: String
    created_at: DateTime<Utc>
}
```

Invariants:
- One user can link multiple providers
- A provider identity can only link to one user

#### RefreshSession

```
RefreshSession {
    id: Uuid
    user_id: Uuid
    token_hash: String
    session_family_id: Uuid
    ip_address: Option<IpAddr>
    user_agent: Option<String>
    device_id: Option<String>
    revoked: bool
    expires_at: DateTime<Utc>
    created_at: DateTime<Utc>
}
```

Invariants:
- Token hash never stored as plaintext
- Rotation: old token revoked → new token issued within same family
- One active session per device (optional hardening)

#### EmailVerification

```
EmailVerification {
    id: Uuid
    user_id: Uuid
    token_hash: String
    expires_at: DateTime<Utc>
    created_at: DateTime<Utc>
}
```

One verification token per user at a time.

#### PasswordReset

```
PasswordReset {
    id: Uuid
    user_id: Uuid
    token_hash: String
    request_ip: Option<IpAddr>
    user_agent: Option<String>
    attempts: u32
    used: bool
    expires_at: DateTime<Utc>
    created_at: DateTime<Utc>
}
```

#### LoginAttempt

```
LoginAttempt {
    id: Uuid
    email: Option<EmailAddress>
    ip_address: Option<IpAddr>
    success: bool
    created_at: DateTime<Utc>
}
```

#### AuthAuditLog

```
AuthAuditLog {
    id: Uuid
    user_id: Option<Uuid>
    event: AuthEvent
    metadata: Option<HashMap<String, Value>>
    created_at: DateTime<Utc>
}
```

### Enums

```
UserRole {
    REGISTERED_DRIVER,
    PARTNER,
    ADMIN,
}

UserStatus {
    ACTIVE,
    SUSPENDED,
    DELETED,
}

OAuthProvider {
    GOOGLE,
    APPLE,
    FACEBOOK,
    MICROSOFT,
}

AuthEvent {
    USER_REGISTERED,
    USER_LOGGED_IN,
    USER_LOGGED_OUT,
    TOKEN_REFRESHED,
    PASSWORD_RESET_REQUESTED,
    PASSWORD_RESET_COMPLETED,
    EMAIL_VERIFIED,
    OAUTH_LINKED,
    OAUTH_UNLINKED,
    ACCOUNT_SUSPENDED,
    ACCOUNT_REACTIVATED,
}
```

### Value Objects

```
EmailAddress(String)          // validated RFC, stored lowercased
PasswordHash(String)          // Argon2id encoded hash
JwtToken {
    access_token: String,
    refresh_token: String,
    expires_in: u64,
}
TokenHash(String)             // SHA-256 or Blake3 of raw token
SessionFamilyId(Uuid)
```

---

## 3 · Driver Domain (driver-service)

### Entities

#### Station

```
Station {
    id: Uuid
    name: String
    address: Option<String>
    latitude: f64
    longitude: f64
    partner_id: Option<Uuid>
    status: StationStatus
    created_at: DateTime<Utc>
    updated_at: DateTime<Utc>
}
```

Geospatial rules:
- Coordinates in SRID 4326
- GIST index required
- Nearby query radius ≤ 5km

#### Connector

```
Connector {
    id: Uuid
    station_id: Uuid
    connector_type: ConnectorType
    power_kw: f64
    status: ConnectorStatus
    price_per_kwh: Option<Money>
    created_at: DateTime<Utc>
    updated_at: DateTime<Utc>
}
```

#### Favorite

```
Favorite {
    id: Uuid
    user_id: Uuid
    station_id: Uuid
    created_at: DateTime<Utc>
}
```

Unique per (user_id, station_id).

#### ChargingSession

```
ChargingSession {
    id: Uuid
    user_id: Uuid
    connector_id: Uuid
    started_at: DateTime<Utc>
    stopped_at: Option<DateTime<Utc>>
    kwh_used: Option<f64>
    cost: Option<Money>
    status: SessionStatus
}
```

#### Review

```
Review {
    id: Uuid
    user_id: Uuid
    station_id: Uuid
    rating: u8                    // 1-5
    comment: Option<String>
    created_at: DateTime<Utc>
    updated_at: DateTime<Utc>
}
```

### Enums

```
ConnectorType {
    TYPE2,        // AC 7-22kW
    CCS,          // DC 50-350kW
    CHADEMO,      // DC 50kW
    TESLA,        // NACS
}

ConnectorStatus {
    AVAILABLE,
    IN_USE,
    OUT_OF_SERVICE,
    MAINTENANCE,
}

StationStatus {
    ACTIVE,
    INACTIVE,
    CLOSED,
}

SessionStatus {
    ACTIVE,
    COMPLETED,
    CANCELLED,
    FAILED,
}
```

### Value Objects

```
Money(u64)                     // centimes (TND)
Latitude(f64)                  // -90 to 90
Longitude(f64)                 // -180 to 180
RadiusKm(f64)                  // 0 to 5
PowerKw(f64)
Rating(u8)                     // 1-5
```

---

## 4 · Admin Domain (admin-service)

### Entities

#### AdminUser

```
AdminUser {
    id: Uuid
    user_id: Uuid               // references auth-service user
    role: AdminRole
    permissions: Vec<Permission>
    created_at: DateTime<Utc>
}
```

#### Partner

```
Partner {
    id: Uuid
    name: String
    contact_email: EmailAddress
    contact_phone: Option<String>
    status: PartnerStatus
    stations_count: u64
    created_at: DateTime<Utc>
    updated_at: DateTime<Utc>
}
```

### Enums

```
AdminRole {
    SUPER_ADMIN,
    STATION_MANAGER,
    SUPPORT,
}

PartnerStatus {
    ACTIVE,
    PENDING_VERIFICATION,
    SUSPENDED,
}

Permission {
    MANAGE_STATIONS,
    MANAGE_PARTNERS,
    MANAGE_USERS,
    VIEW_ANALYTICS,
    MANAGE_PRICING,
}
```

---

## 5 · Aggregate Boundaries

| Aggregate | Root Entity | Related Entities |
|---|---|---|
| User | User | OAuthAccount, RefreshSession, EmailVerification, PasswordReset |
| Station | Station | Connector, Review |
| ChargingSession | ChargingSession | — |
| Partner | Partner | — |

---

## 6 · Domain Events (Future)

Placeholder for eventual event sourcing / integration events:

```
StationActivated { station_id }
StationDeactivated { station_id }
ChargingStarted { session_id, user_id, connector_id }
ChargingCompleted { session_id, kwh_used, cost }
UserRegistered { user_id, email, role }
```

---

## 7 · Service-Domain Mapping

| Service | Domain Module |
|---|---|
| auth-service | `auth_service::domain` → re-exports from `bornemap-core::auth` |
| driver-service | `driver_service::domain` → re-exports from `bornemap-core::driver` |
| admin-service | `admin_service::domain` → re-exports from `bornemap-core::admin` |
| shared | `bornemap-core` defines all domain types |

---

## 8 · Validation Rules

| Field | Rule |
|---|---|
| Email | RFC 5321, max 254 chars |
| Password | min 8 chars, max 128 chars |
| Latitude | [-90, 90] |
| Longitude | [-180, 180] |
| Radius | (0, 5] km |
| Rating | [1, 5] |
| Power | > 0 kW |
| Money | ≥ 0 centimes |
