# ADR-008: No ngrok Usage

**Date:** 2026-06-11
**Status:** Accepted
**Decision:** ngrok is prohibited for local development.

---

## Context

ngrok provides public URLs for local development, which can lead to accidental exposure of internal services. It also creates dependency on external services and inconsistent URLs across environments.

---

## Decision

**Use direct IP or Cloudflare Tunnel for local development instead of ngrok.**

### Allowed Alternatives

1. **Direct IP:** `http://localhost:8080/api/v1/stations`
2. **Cloudflare Tunnel:** `https://bornemap.local/api/v1/stations`
3. **Docker Network:** Internal Docker DNS for service discovery

### Local Dev Setup

```yaml
# docker-compose.yml
services:
  driver-service:
    ports:
      - "8080:8080"
    networks:
      - bornemap

  admin-service:
    ports:
      - "8081:8081"
    networks:
      - bornemap

networks:
  bornemap:
    driver: bridge
```

**Usage:**
```bash
# Direct access
curl http://localhost:8080/api/v1/stations

# From mobile app
# Add to app config or use development server
```

---

## Consequences

### Positive
- No external dependencies for local dev
- No risk of accidental public exposure
- Consistent URLs across environments
- Faster startup (no tunnel registration)

### Negative
- Requires direct IP setup (though simple)
- No automatic public URL sharing (use tunnel for demo)

---

## Alternatives Considered

### Alternative 1: ngrok
```bash
ngrok http 8080
# Exposes to public internet
# Requires ngrok account
# Risk of accidental exposure
```

**Rejected:** Security risk, external dependency, inconsistent URLs.

### Alternative 2: Localhost Tunnel (official)
```bash
npx localtunnel --port 8080
```

**Rejected:** Similar to ngrok, still requires external service.

### Alternative 3: Docker Network DNS
```
docker-compose.yml:
  networks:
    bornemap:
      driver: bridge
```

**Used:** Preferred method for MVP-1.

---

## Implementation

1. Remove ngrok from project dependencies
2. Document direct IP setup in README
3. Add Cloudflare Tunnel option for demos
4. Update API client configuration

---

## Testing

- Verify local services accessible via `http://localhost:8080`
- Verify mobile app can connect to local services
- Verify no ngrok process running during development

---

## References

- **Constitution:** Section 6.5 — Network
- **API Gateway:** ADR-001 — Traefik as API Gateway
