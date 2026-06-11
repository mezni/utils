# Implementation Plan: Clickstream Service — Event Ingestion

**Branch**: `004-clickstream-service` | **Date**: 2026-06-11 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `/specs/004-clickstream-service/spec.md`

## Summary

Implement an Actix-web REST API (port 8082) that ingests clickstream events (single and batch) into `analytics_db` (PostgreSQL, port 5433). Events are validated against the MVP-1 taxonomy, stored in `raw_events`, and always return 202 Accepted (fire-and-forget pattern). Per-IP rate limiting prevents abuse. The service lives under `source/services/clickstream-service/` as a workspace member.

## Technical Context

**Language/Version**: Rust 1.96

**Primary Dependencies**: actix-web 4, serde + serde_json, tokio, tracing + tracing-actix-web, sqlx (PostgreSQL), nanoid, actix-governor (rate limiting)

**Storage**: PostgreSQL (analytics_db, port 5433), table `raw_events` managed via sqlx embedded migrations

**Testing**: cargo test (unit + integration with testcontainers or mocked DB)

**Target Platform**: Linux server (x86_64)

**Project Type**: web-service (REST API)

**Performance Goals**: Single event <100ms (99%), batch of 100 <500ms, 500 concurrent connections with p95 <200ms

**Constraints**: Fire-and-forget (always 202 regardless of DB state), max 64KB/event, 512KB/batch, max 100 events/batch, per-IP rate limiting (429 when exceeded)

**Scale/Scope**: MVP-1 with 6 event types (`map_open`, `station_view`, `station_click`, `nearby_search`, `map_pan`, `map_zoom`). Single service, no auth, no event transformation.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Constitution is a template-only file (`.specify/memory/constitution.md` is unpopulated). No constitution gates to evaluate.

## Project Structure

### Documentation (this feature)

```text
specs/004-clickstream-service/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output
└── tasks.md             # Phase 2 output (/speckit.tasks)
```

### Source Code (repository root)

```text
source/
└── services/
    └── clickstream-service/
        ├── Cargo.toml
        ├── src/
        │   ├── main.rs            # Server bootstrap, routes, middleware
        │   ├── routes/
        │   │   ├── mod.rs
        │   │   ├── ingest.rs      # POST /api/v1/events, /api/v1/events/batch
        │   │   └── health.rs      # GET /api/v1/health
        │   ├── models/
        │   │   ├── mod.rs
        │   │   └── event.rs       # Event, EventBatch, validation
        │   ├── db/
        │   │   ├── mod.rs
        │   │   └── repository.rs  # AnalyticsDbRepo (insert_event, insert_batch)
        │   ├── middleware/
        │   │   ├── mod.rs
        │   │   └── rate_limiter.rs # Per-IP token bucket
        │   ├── errors.rs          # AppError, JSON error responses
        │   └── response.rs        # Envelope: { data, error, meta }
        └── migrations/
            ├── 001_create_raw_events.sql
            └── ...
```

**Structure Decision**: Single Actix-web service under `source/services/clickstream-service/`, consistent with `source/services/driver-service/`. Embedded sqlx migrations for analytics_db schema.

## Complexity Tracking

> No Constitution violations — complexity justification not required.
