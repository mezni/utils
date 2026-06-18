# Contract: GET /health

## Purpose

Health check endpoint for Traefik and deployment orchestration probes.

## Request

`GET /health`

No query parameters or body required.

## Response

### 200 OK — Healthy

```json
{
  "status": "ok"
}
```

Returned when the database connection pool can acquire a connection within 500ms.

### 503 Service Unavailable — Degraded

```json
{
  "status": "degraded"
}
```

Returned when the database connection pool is exhausted, unreachable, or times out during acquire.

## Errors

| HTTP Status | Condition |
|-------------|-----------|
| 503 | Database unreachable or pool exhausted |

## Response Fields

| Field | Type | Always Present | Description |
|-------|------|----------------|-------------|
| `status` | string | yes | `"ok"` or `"degraded"` |
