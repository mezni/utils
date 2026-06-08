# Decisions

## DEC-001 — Move Dockerization Into MVP-1
Date: 2026-06-08
Context: MVP-1 needs a reproducible, on-ramp-friendly runtime so the FastAPI service and the three frontend apps can be started the same way across machines while the first end-to-end loop is being built.
Decision: Dockerization is part of MVP-1 scope, including Dockerfiles and a local Docker Compose setup for development and onboarding, while keeping production hardening, Traefik, TLS, and launch readiness in later MVPs.
Alternatives considered: Keep dockerization in MVP-6 only; introduce only host-run tooling for MVP-1.
Consequences: MVP-1 now includes container setup work alongside the core feature loop. The phase plan must treat Dockerfiles and MVP-1 Compose setup as required deliverables, while preserving the current exclusions for production exposure and platform hardening.

## DEC-002 — Keep All Code Under `source/`
Date: 2026-06-08
Context: The repository needs a single, predictable root for all runnable code so service, app, and shared package layout stays consistent across MVPs.
Decision: All codebase files that contain runnable application, service, shared package, or test code must live under `source/`.
Alternatives considered: Keep root-level application folders; split code across multiple top-level directories.
Consequences: Future implementation work must place apps, services, packages, and their tests under `source/`, while keeping docs, plans, and project tracking files at the repository root or in `docs/`.
