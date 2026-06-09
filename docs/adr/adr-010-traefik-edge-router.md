# ADR-010: Traefik as edge router (from MVP-6)

**Status:** Accepted
**Date:** 2026-06-09

## Context

Production deployment requires TLS termination, HTTP to HTTPS redirect, and routing to multiple backend services. Docker Compose does not provide these capabilities natively. An edge reverse proxy is needed.

## Decision

Use Traefik v3 as the edge router. Only Traefik exposes public ports (80, 443). All other services use internal Docker networking. Traefik handles TLS via Let's Encrypt, HTTP to HTTPS redirect, and service routing via Docker labels.

## Consequences

- Automatic TLS with Let's Encrypt
- Docker-native service discovery via labels
- Middleware support for rate limiting and headers
- Single entry point simplifies security posture
- Additional container to manage
