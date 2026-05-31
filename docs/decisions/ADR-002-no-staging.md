# ADR-002: No Staging Environment

## Status

Accepted

## Context

Operating a staging environment doubles infrastructure cost and
maintenance effort for a small team.

## Decision

Deploy directly to production via Docker Compose. Rollback is
deterministic via image tag reversal. CI runs pass/fail before any
deployment proceeds.

## Consequences

- Faster iteration with no staging gate
- Lower infrastructure costs
- Requires thorough CI pipeline (lint, test, build, integration)
- Rollback must be reliable and tested
- Feature flags may be needed for risky changes
