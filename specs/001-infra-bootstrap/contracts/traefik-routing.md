# Traefik Routing Contract

**Files**: `source/infra/traefik/traefik.yml` (static), `source/infra/traefik/dynamic/routing.yml` (dynamic)

## Static Configuration

```yaml
entryPoints:
  web:
    address: ":80"

providers:
  file:
    filename: /etc/traefik/dynamic/routing.yml
    watch: true
```

## Dynamic Routing Rules

| Priority | Path Prefix | Middleware | Service | URL |
|----------|-------------|-----------|---------|-----|
| 10 | `/api/v1/auth/` | — | auth-service | `http://auth-service:3000` |
| 10 | `/api/v1/admin/` | — | admin-service | `http://admin-service:3002` |
| 10 | `/api/v1/driver/` | — | driver-service | `http://driver-service:3001` |

## 404 Catch-All

All unmatched paths return `404 Not Found` with body `{"error":"route_not_found"}`.

## No Auth Middleware (Sprint 0)

Sprint 0 routes have **no JWT validation**. Authentication middleware is added in Sprint 3. This is explicitly declared as an assumption.

## Service URL Format

Services are referenced by Docker Compose service name (DNS resolution within `bornemap-net`):

```
http://<service-name>:<container-port>
```

## Stub Containers

Each stub returns a static JSON response for any path:

```
auth-service stub → {"service":"auth-service","status":"stub"}
admin-service stub → {"service":"admin-service","status":"stub"}
driver-service stub → {"service":"driver-service","status":"stub"}
```

## Future Middleware Order (Sprint 3+)

```
Request → Rate Limit → JWT Validation → Header Injection → Backend
```
