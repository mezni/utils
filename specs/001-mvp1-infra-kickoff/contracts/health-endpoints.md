# Health Endpoint Contracts

## `GET /api/v1/health`

Liveness check. No auth. All three services (auth, driver, admin).

**Request**: None

**Response 200**:
```json
{
  "status": "ok",
  "service": "auth-service",
  "version": "0.1.0"
}
```

## `GET /api/v1/health/ready`

Readiness check — verifies database connectivity. No auth.

**Response 200**:
```json
{
  "status": "ready"
}
```

**Response 503**:
```json
{
  "status": "not ready",
  "error": "DB connection failed"
}
```

## Service Ports

| Service | Port |
|---------|------|
| auth-service | 3000 |
| driver-service | 3001 |
| admin-service | 3002 |
| gis-service | 3003 (MVP-2) |

## Environment Variables

All services accept:

| Variable | Default | Required |
|----------|---------|----------|
| `HOST` | `0.0.0.0` | No |
| `PORT` | per service | No |
| `DATABASE_URL` | — | Yes |
| `LOG_LEVEL` | `info` | No |
