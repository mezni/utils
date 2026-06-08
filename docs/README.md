# BorneMap Documentation

Navigation guide for all project documentation.

## Core Documents

Start here to understand the project:

- **[constitution.md](constitution.md)** — Permanent rules, principles, roles, architecture, and non-negotiable constraints. The source of truth for all decisions.
- **[implementation-plan.md](implementation-plan.md)** — Delivery strategy, MVP roadmap, sprint breakdown, and done criteria.
- **[out-of-scope-registry.md](out-of-scope-registry.md)** — Features explicitly deferred indefinitely (OCPP, payments, routing, real-time, push notifications).

## Architecture Decisions

All accepted decisions recorded as ADRs. Start with the index in `constitution.md` section 14.

- **[docs/adr/](adr/)** — Architecture Decision Records
  - [ADR-001: PostgreSQL as single database](adr/ADR-001-postgresql-single-database.md)
  - [ADR-016: Python FastAPI for MVP-1](adr/ADR-016-python-fastapi-mvp1.md)
  - [ADR-011: React + Vite for web](adr/ADR-011-react-vite-web.md)
  - [ADR-012: React Native + Expo SDK 54](adr/ADR-012-react-native-expo-mobile.md)
  - [ADR-014: Leaflet + OpenStreetMap](adr/ADR-014-leaflet-openstreetmap.md)
  - *(More ADRs to be created as project evolves)*

## Project Planning

- **[docs/project/backlog.md](project/backlog.md)** — Feature backlog organized by MVP
- **[docs/project/bugs.md](project/bugs.md)** — Bug tracker with classification system
- **[docs/project/decisions.md](project/decisions.md)** — Small decisions that don't rise to ADR level
- **[docs/project/phases/mvp-01-status.md](project/phases/mvp-01-status.md)** — Current MVP phase status, sprint breakdown, and done criteria

## API and Database

- **[docs/api/bornemap-service.md](api/bornemap-service.md)** — Full API endpoint reference for MVP-1 (Health, Partners, Stations, Chargers)
- **[docs/schema/inventory-schema.md](schema/inventory-schema.md)** — Database schema for `inventory` (partner, station, charger tables)

## Guides

*(To be created as project progresses)*

- `docs/guides/onboarding.md` — How to run the full stack locally
- `docs/guides/event-taxonomy.md` — Analytics event canonical taxonomy (MVP-5)

## Operations

*(To be created as project advances beyond MVP-1)*

- `docs/ops/keycloak-setup.md` — Keycloak deployment and configuration (MVP-3)
- `docs/ops/osm-import.md` — OSM data import procedure (MVP-4)

## Design System

*(Reference only; actual tokens in source/packages/ui)*

- `docs/design/` — Design system specification (color tokens, typography, spacing, layout patterns)

---

## How to Use This Documentation

### For New Team Members

1. Read `constitution.md` sections 1–2 (purpose, principles)
2. Read `AGENTS.md` (quick start guide)
3. Read `implementation-plan.md` (what we're building)
4. Read current phase status in `docs/project/phases/mvp-XX-status.md`

### For Architecture Decisions

1. Check `constitution.md` section 14 (ADR index)
2. Read the relevant ADR (e.g., ADR-011 for web tech)
3. If not found, propose new decision in `docs/project/decisions.md`
4. If major decision, create new ADR in `docs/adr/`

### For Development

1. Check `docs/api/bornemap-service.md` for endpoint contracts
2. Check `docs/schema/inventory-schema.md` for data model
3. Check `docs/project/decisions.md` for MVP-level choices (e.g., color logic, validation)
4. Check current phase status for sprint assignments and exit criteria

### For Bugs

1. Report in `docs/project/bugs.md` with classification (Class A, B, C)
2. Link to related ADR or constitution section if applicable

---

## Document Ownership

- **constitution.md**: Source of truth for all rules. Edited only when principles change. Never incrementally modified.
- **implementation-plan.md**: Edited at the start of each MVP phase. Sprints recorded as they complete.
- **ADRs**: Accepted ADRs are never edited. Superseded decisions get new ADRs that reference the old one.
- **decisions.md**: Small decisions recorded before code. Never edited once recorded.
- **Phase status files**: Updated weekly during sprints. Archived when MVP closes.
- **API and schema docs**: Updated as endpoints and tables are implemented.

---

## Key Principles

1. **Single source of truth**: Constitution is authoritative. No contradictions elsewhere.
2. **Decisions before code**: All choices recorded in constitution, ADRs, or decisions file before implementation.
3. **No editing approved decisions**: Decisions are permanent. Superseding decisions are new, not edits.
4. **Executable sources of truth**: Migrations, API contracts, and schema are canonical; prose is explanatory.
5. **Transparency**: Every major decision is documented. No surprises on review.

---

**Last Updated**: Sprint 1.1 (in progress)
