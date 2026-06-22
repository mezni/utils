# Audit Event Contract

**Purpose**: Define the event schema for authentication audit events sent from auth-service to driver-service for writing to analytics_db.

## Event Types

| Event Type | Description | Payload Fields |
|------------|-------------|----------------|
| auth.login_success | Successful authentication | user_uuid, email, role, ip_address, timestamp |
| auth.login_failure | Failed authentication attempt | user_uuid (if known), email, reason, ip_address, timestamp |
| auth.token_rejected | Rejected JWT (expired/invalid) | user_uuid (if extractable), reason, token_jti, ip_address, timestamp |
| auth.logout | User logout | user_uuid, timestamp |

## HTTP Contract

**Endpoint**: `POST /api/v1/telemetry/events` (driver-service)

**Request Body**:
```json
{
    "event_type": "auth.login_success",
    "source_service": "auth-service",
    "event_data": {
        "user_uuid": "550e8400-e29b-41d4-a716-446655440000",
        "email": "user@example.com",
        "role": "driver",
        "ip_address": "192.168.1.100"
    },
    "idempotency_key": "auth-login-success-550e8400-1712345678"
}
```

**Response** (201 Created):
```json
{
    "status": "accepted",
    "event_id": "EVT_aB3dEfGhIjKl"
}
```

## Idempotency Key Convention

Format: `{event_type}-{user_uuid}-{unix_timestamp}`

Example: `auth-login-success-550e8400-e29b-41d4-a716-446655440000-1712345678`

## Domain-Types Representation

```rust
pub struct AuditEvent {
    pub event_type: String,
    pub source_service: String,
    pub event_data: serde_json::Value,
    pub idempotency_key: String,
}
```
