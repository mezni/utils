# Health API Contract

## Overview

The health API provides liveness and readiness endpoints for the core-service
backend. Used by orchestration tools (Docker, k8s) and by the mobile app to
verify backend connectivity.

## Base URL

All endpoints are under `/api/v1`.

## Endpoints

### GET /api/v1/health/live

Liveness check — confirms the service process is running.

**Response 200 OK**:
```json
{
  "status": "alive",
  "service": "core-service"
}
```

### GET /api/v1/health/ready

Readiness check — confirms the service can accept traffic. In Phase 1 this is
identical to liveness since no external dependencies exist.

**Response 200 OK**:
```json
{
  "status": "ready",
  "service": "core-service"
}
```

## Error Responses

| Status | When |
|--------|------|
| 404 | Route not found |
| 500 | Internal server error |

## Mobile App Contract

The mobile-driver app MUST call `/api/v1/health/live` on startup and display
the response status. If the endpoint is unreachable, the app MUST display a
"Connection Error" message with a retry prompt.

## Future Considerations

- Readiness should check database connectivity when a database is added
- Response schema may expand with `version`, `uptime`, `dependencies` fields
