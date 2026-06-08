# ADR-016: Python FastAPI for MVP-1 Backend

**Status**: Accepted

**Date**: 2026-01-01

## Context

MVP-1 requires a backend service to expose 16 CRUD endpoints and handle station discovery. The MVP must ship quickly with minimal infrastructure. Python and Rust are both viable options.

## Decision

**Use Python FastAPI for MVP-1 backend.**

The service runs at `source/services/bornemap-service/`, port 8000, and is replaced by Rust services in MVP-2.

## Rationale

- **Speed to MVP**: Python + FastAPI + SQLAlchemy can deliver 16 endpoints in 2 weeks.
- **Team familiarity**: Python is accessible to rapid prototyping teams.
- **Not permanent**: FastAPI is a transient choice. Rust services replace it in MVP-2 without breaking the API contract.
- **No premature optimization**: MVP-1 is not at scale. Python is sufficient for validation.
- **Full-stack learning**: Team validates the data model and product before investing in Rust infrastructure.

## Consequences

- All 16 MVP-1 endpoints must maintain `/api` prefix and clear request/response contracts for migration to Rust.
- Database migrations written in Alembic (SQL-based) are canonical and remain even after Python service is retired.
- Frontend apps must not depend on Python-specific behavior. All behavior is via HTTP/JSON contracts.
- Technology lock-in risk is explicitly mitigated: code is thrown away in MVP-2, not refactored.

## Superseded By

ADR-005: Rust + Actix-web for backend services (MVP-2 onward).

## References

- Implementation Plan, Sprint 1.1: Backend and Database
- Constitution section 2, Principle 1: MVP-first Delivery
- Constitution section 2, Principle 2: Layered Complexity
