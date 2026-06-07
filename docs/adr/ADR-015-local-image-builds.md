# ADR-015: Local Image Builds — No Image Registry

**Status**: Accepted
**Date**: 2026-06-07

## Context

Docker images for Rust services need to be built and deployed. Options: push to a container registry (Docker Hub, GHCR, private registry) and pull on the host, or build directly on the host.

## Decision

Build Docker images locally on the production host. No image registry is used.

## Rationale

- Simplifies infrastructure: no registry credentials, no registry configuration, no registry downtime
- Single host deployment means building on the host is fast (no image download)
- Principle 3 (Simple operations): one less system to manage
- The deployment runbook is simpler: `git pull && docker compose build && docker compose up -d`
- CI does not need registry credentials

## Consequences

- Container images cannot be versioned or audited centrally
- Each host builds images independently (no sharing)
- Build failures block deployment (mitigated by CI verifying builds first)
- Slower deploys than pulling pre-built images

## Compliance

- Images are built on the host during deployment
- No image registry may be introduced without an ADR
- CI validates the build — deployment only runs if CI passes
