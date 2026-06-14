# ADR-0001: Static System Boundary for MVP-1 Auth Deferral

## Status

Accepted

## Context

MVP-1 does not include authentication infrastructure. However, the system requires
a stable user identifier for session context in UseCase orchestrators to avoid
partial infrastructure implementations during early development.

## Decision

Hardcode the application fallback identifier `usr-mvp1-fallback` inside UseCase
orchestrators for all session context fields until authentication is implemented
in a later milestone. This keeps the boundary between MVP-1 and future auth
explicitly defined at the orchestrator level rather than leaking infrastructure
concerns upward.

## Consequences

- Positive: Clean separation of concerns — no partial auth middleware in MVP-1.
- Positive: Single search target (`usr-mvp1-fallback`) to replace when auth lands.
- Negative: Production deployments must override via configuration or environment.
