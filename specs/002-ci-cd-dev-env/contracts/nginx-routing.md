# Contract: NGINX Gateway Routing

**Path**: `nginx/default.conf`
**Consumers**: developers running the local stack; all backend services
**Source**: spec FR-008, FR-013, research R-008

## Routing table

| Path prefix | Upstream service | Port | Notes |
|-------------|-----------------|------|-------|
| `/auth/` | auth-service | 3000 | Strips `/auth/` prefix before forwarding |
| `/api/core/` | core-service | 3001 | Strips `/api/core/` prefix |
| `/api/geo/` | geo-service | 3002 | Strips `/api/geo/` prefix |
| `/api/analytics/` | analytics-service | 3003 | Strips `/api/analytics/` prefix |
| `/health` | all services | — | Proxies to each service's `/health` endpoint |
| `/metrics` | all services | — | Proxies to each service's `/metrics` endpoint |

## Gateway behavior

- Listen on port 80 (HTTP) for local development.
- TLS (HTTPS) is out of scope for Phase 1 — deferred to Phase 11.
- Unknown paths return 404.
- If a service is down, the gateway returns 502 for that service's routes.

## Health endpoint routing

The gateway MUST expose a consolidated `/health` endpoint that fans out to each service:

```nginx
location /health/ {
    rewrite ^/health/(.*) /$1 break;
    proxy_pass http://$upstream;
}
```

Individual service health checks remain accessible at `/health/auth-service`, `/health/core-service`, etc.

## Metrics endpoint routing

The gateway MUST expose a consolidated `/metrics` endpoint following the same pattern as health.

## Non-goals

- Rate limiting — deferred to Phase 11 hardening.
- Request logging — individual services handle structured logging (Principle VI).
- CORS — handled at the service or frontend level.
