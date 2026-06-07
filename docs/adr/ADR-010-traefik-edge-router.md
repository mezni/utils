# ADR-010: Traefik as Edge Router

**Status**: Accepted
**Date**: 2026-06-07

## Context

The platform needs an edge router for TLS termination, HTTP routing, and rate limiting. Options: Nginx, HAProxy, Caddy, Traefik, or cloud load balancer.

## Decision

Use Traefik v3 as the edge router.

## Rationale

- Native Docker Compose integration — auto-discovers containers via labels
- Automatic Let's Encrypt TLS certificate management
- Built-in rate limiting
- Dashboard for monitoring
- Single binary, lightweight container
- Configured via Docker labels or dynamic config — no Nginx-style config files

## Consequences

- Traefik is the only component with public port exposure
- Internal services are not directly accessible from outside
- TLS certificate renewal is handled automatically
- Custom configuration (beyond labels) requires dynamic configuration file

## Compliance

- Traefik is the only public-facing container
- No other container exposes host ports
- All external routing is defined in Traefik configuration
