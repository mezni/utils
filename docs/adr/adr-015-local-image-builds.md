# ADR-015: Local image builds — no registry

**Status:** Accepted
**Date:** 2026-06-09

## Context

Backend services need to be packaged as Docker images for deployment. Image registries (Docker Hub, ECR, GCR) add a dependency on external infrastructure and complicate the simple deployment model.

## Decision

Build Docker images locally on the host. No image registry. Deployment uses `docker compose build` directly on the server. Images are never pushed — always built from source.

## Consequences

- Zero dependency on external registry infrastructure
- Simpler security model — no registry credentials to manage
- Build time on every deploy (mitigated by Docker layer caching)
- Consistent with the bare metal / simple operations philosophy
