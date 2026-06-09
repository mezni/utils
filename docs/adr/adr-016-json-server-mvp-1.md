# ADR-016: json-server for MVP-1 mock API

**Status:** Accepted
**Date:** 2026-06-09

## Context

MVP-1 needs a working API in days, not weeks, to validate the full product loop. Building Rust services with PostgreSQL from day one would delay frontend development. The API must serve all four resource types (partners, stations, chargers, availability) under the /api prefix.

## Decision

Use json-server as the MVP-1 mock API. Data lives in `source/mock/db.json`. Routes are mapped via `source/mock/routes.json` to add the /api prefix. All three frontend apps target the json-server base URL. json-server is replaced entirely by Rust services in MVP-2.

## Consequences

- Functional API within hours instead of weeks
- Supports filtering, pagination (limited), POST, PUT, PATCH, DELETE
- Frontend apps developed against a stable, predictable API
- No database, no migrations, no infrastructure needed
- All API consumers change base URL once when Rust services replace it
