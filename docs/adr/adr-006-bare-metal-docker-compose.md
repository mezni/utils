# ADR-006: Bare metal + Docker Compose over Kubernetes

**Status:** Accepted
**Date:** 2026-06-09

## Context

BorneMap must be operable by one person (Principle 5). Kubernetes adds significant operational complexity (cluster management, networking, monitoring, upgrades). The platform has a small number of services that can be managed more simply.

## Decision

Use bare metal or VPS with Docker Compose for deployment. No Kubernetes. Traefik serves as the edge router (MVP-6+). Images are built locally on the host — no image registry. Deployment is manual by runbook.

## Consequences

- Simpler operations — one person can manage the entire stack
- No Kubernetes API, RBAC, or networking complexity
- Scaling is manual (vertical or horizontal with load balancer)
- Zero-downtime deploys require careful orchestration via Traefik
