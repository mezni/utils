# Error Catalog

## Response Format

```json
{
  "error": {
    "code": "ERR_001",
    "message": "Human-readable description",
    "details": {}  // optional, per-error context
  }
}
```

## Auth Service Errors (`AUTH_*`)

| Code | HTTP Status | Message | When |
|------|-------------|---------|------|
| `AUTH_001` | 409 | Email already registered | Registration with existing email |
| `AUTH_002` | 422 | Password does not meet strength requirements | Password < 8 chars, no uppercase, etc. |
| `AUTH_003` | 401 | Invalid access token | JWT expired, bad signature, or malformed |
| `AUTH_004` | 403 | Insufficient permissions | Valid token but wrong realm/role for endpoint |
| `AUTH_005` | 500 | Keycloak communication failed | Auth Service cannot reach Keycloak |
| `AUTH_006` | 403 | Partner account pending approval | Partner tries to use dashboard before admin approves |
| `AUTH_007` | 403 | Partner registration rejected | Partner tries to login after rejection |

Note: login, social login, token refresh, and logout are handled directly by Keycloak OIDC endpoints. Error codes for those flows come from Keycloak directly, not Auth Service.

## Station Errors (`STA_*`)

| Code | HTTP Status | Message | When |
|------|-------------|---------|------|
| `STA_001` | 404 | Station not found | Station ID does not exist or is deleted |
| `STA_002` | 422 | Station validation failed | Required fields missing or invalid |
| `STA_003` | 403 | Partner not found or inactive | Partner ID references non-existent/suspended partner |
| `STA_004` | 403 | Not authorized to modify this station | Token's partner_id doesn't match station's partner_id |
| `STA_005` | 422 | Invalid station status transition | e.g. draft -> closed without going through active |
| `STA_006` | 409 | Station has active chargers, cannot close | Attempting to close station with active chargers |

## Charger Errors (`CHG_*`)

| Code | HTTP Status | Message | When |
|------|-------------|---------|------|
| `CHG_001` | 422 | Charger validation failed | Invalid type/connector/power combination |
| `CHG_002` | 404 | Charger not found | Charger ID does not exist |
| `CHG_003` | 403 | Station does not have this charger | Charger ID not under the specified station |

## GIS / Geo Errors (`GEO_*`)

| Code | HTTP Status | Message | When |
|------|-------------|---------|------|
| `GEO_001` | 422 | Invalid coordinates | Lat/lon out of valid range |
| `GEO_002` | 422 | Radius exceeds maximum | radius_m > 50000 |
| `GEO_003` | 503 | GIS service unavailable | Database or cache unavailable |
| `GEO_004` | 503 | Cache unavailable, degraded mode | Redis unreachable, serving from DB |

## GIS Cache Errors (`GIS_*`)

| Code | HTTP Status | Message | When |
|------|-------------|---------|------|
| `GIS_001` | 422 | No station_ids provided | Cache-bust request missing required field |
| `GIS_002` | 502 | Cache invalidation failed | Redis unreachable during invalidation |

## Favorite Errors (`FAV_*`)

| Code | HTTP Status | Message | When |
|------|-------------|---------|------|
| `FAV_001` | 409 | Station already in favorites | Duplicate favorite entry |

## Partner Errors (`OPR_*`)

| Code | HTTP Status | Message | When |
|------|-------------|---------|------|
| `OPR_001` | 404 | Partner not found | |
| `OPR_002` | 422 | Partner validation failed | |
| `OPR_003` | 409 | Partner email already exists | Duplicate partner registration |
| `OPR_004` | 403 | Partner is suspended | Action blocked due to partner status |
| `OPR_005` | 422 | Partner not in pending status | Approve/reject attempted on non-pending partner |

## General Errors (`GEN_*`)

| Code | HTTP Status | Message | When |
|------|-------------|---------|------|
| `GEN_001` | 400 | Invalid request body | Malformed JSON |
| `GEN_002` | 400 | Missing required parameter | Query param or path param missing |
| `GEN_003` | 404 | Resource not found | Generic not found |
| `GEN_004` | 405 | Method not allowed | Wrong HTTP method for endpoint |
| `GEN_005` | 429 | Too many requests | Rate limit exceeded (MVP-6+) |
| `GEN_500` | 500 | Internal server error | Unhandled exception — no details leaked to client |
| `GEN_503` | 503 | Service unavailable | Dependency (DB, Keycloak) unreachable |

## Error Code Ranges by Service

| Service | Code Prefix | Defined Range |
|---------|-------------|---------------|
| Auth Service | `AUTH` | `AUTH_001` – `AUTH_099` |
| Driver Service | `STA`, `CHG`, `FAV` | `STA_001` – `STA_099`, `CHG_001` – `CHG_099`, etc. |
| Admin Service | `OPR`, `STA` (admin prefix) | `OPR_001` – `OPR_099` |
| GIS Service | `GEO`, `GIS` | `GEO_001` – `GEO_099`, `GIS_001` – `GIS_099` |
| General | `GEN` | `GEN_001` – `GEN_999` |

## Error Response Headers

- `X-Request-Id`: correlation ID (set by Traefik in MVP-6+; use random UUID before that)
- `Content-Type`: `application/json`
