# Audit Event Contract

**Purpose**: Define the event schema for authentication audit events sent from auth-service to driver-service for writing to analytics_db.

## Event Types

| Event Type | Description | Payload Fields |
|------------|-------------|----------------|
| auth.login_success | Successful authentication | user_uuid, email, role, ip_address, user_agent, correlation_id, timestamp |
| auth.login_failure | Failed authentication attempt | user_uuid (if known), email, reason, ip_address, user_agent, correlation_id, timestamp |
| auth.token_rejected | Rejected JWT (expired/invalid) | user_uuid (if extractable), reason, token_jti, ip_address, correlation_id, timestamp |
| auth.logout | User logout | user_uuid, correlation_id, timestamp |
| auth.access_denied | RBAC rejection | user_uuid, role, resource, required_role, ip_address, correlation_id, timestamp |
| auth.role_change_detected | Role change during JIT | user_uuid, old_role, new_role, correlation_id, timestamp |
| auth.jit_user_created | First-time user provisioned | user_uuid, email, role, correlation_id, timestamp |
| auth.jit_user_updated | Existing user profile updated | user_uuid, changed_fields, correlation_id, timestamp |
| auth.refresh_token_rejected | Refresh token rejected | user_uuid, reason, correlation_id, timestamp |

## HTTP Contract

**Endpoint**: `POST /api/v1/telemetry/events` (driver-service)

**Authentication**: Service account client credentials (Keycloak client_credentials grant)

**Request Body**:
```json
{
    "event_type": "auth.login_success",
    "source_service": "auth-service",
    "event_data": {
        "user_uuid": "550e8400-e29b-41d4-a716-446655440000",
        "email": "user@example.com",
        "role": "driver",
        "ip_address": "192.168.1.100",
        "user_agent": "Mozilla/5.0 ...",
        "correlation_id": "corr-a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    },
    "idempotency_key": "auth-login-success-550e8400-1712345678"
}
```

**Response** (201 Created):
```json
{
    "status": "accepted",
    "event_id": "EVTaB3dEfGhIjKl"
}
```

**Error Responses**:
- 401 — Invalid or missing service account credentials
- 422 — Invalid event schema
- 409 — Duplicate event (idempotency_key already processed)

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

pub struct SecurityEventData {
    pub user_uuid: Uuid,
    pub role: Role,
    pub ip_address: Option<String>,
    pub user_agent: Option<String>,
    pub correlation_id: String,
    pub reason: Option<String>,
}
```
