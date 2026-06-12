# Core Files Index

## Overview

This index provides quick access to all core files that prevent LLM from making inconsistent decisions in the BorneMap project.

---

## Quick Links

| File | Priority | Status | Description |
|------|----------|--------|-------------|
| [`docs/core-files-importance.md`](./core-files-importance.md) | 🟢 Critical | ✅ Done | Detailed reasoning for each file |
| [`docs/core-files-summary.md`](./core-files-summary.md) | 🟢 Critical | ✅ Done | Quick reference summary |
| [`docs/mvp/mvp-1-discovery-core.md`](./mvp/mvp-1-discovery-core.md) | 🟢 Critical | ✅ Done | MVP task list and scope |

---

## Database Schema Files

### Infrastructure Migrations

| File | Priority | Location | Description |
|------|----------|----------|-------------|
| `001_platform_db_init.sql` | 🟢 Critical | `infra/migrations/` | Stations + chargers DDL with PostGIS |
| `003_analytics_db_init.sql` | 🟢 Critical | `infra/migrations/` | Raw events table with append-only constraints |
| `004_seed_stations.sql` | 🟢 Critical | `infra/migrations/` | Tunisia seed data with real coordinates |

**Why Critical:** These define the exact schema the LLM must follow. Without them, the LLM invents column names, types, and relationships.

---

## Infrastructure Files

| File | Priority | Location | Description |
|------|----------|----------|-------------|
| `docker-compose.yml` | 🟢 Critical | `infra/` | Local service configuration (ports, volumes, networks) |
| `.env.example` | 🟢 Critical | `infra/` | Environment variable registry and documentation |

**Why Critical:** These provide the exact configuration the LLM must use. Without them, the LLM invents port mappings, volume names, and env var names.

---

## Design System Files

| File | Priority | Location | Description |
|------|----------|----------|-------------|
| `design/tokens.ts` | 🟢 Critical | `source/mobile-driver/design/` | Design tokens (colors, spacing, typography) |
| `design/theme.ts` | 🟢 Critical | `source/mobile-driver/design/` | Dark/light theme objects and switching logic |

**Why Critical:** These enforce Rule 11 (no hardcoded values). Without them, the LLM hardcodes colors, spacing, and typography across components.

---

## Documentation Files

### Architecture Decision Records (ADRs)

| ADR | Priority | Location | Description |
|-----|----------|----------|-------------|
| ADR-001 | 🟡 Important | `docs/architecture/adr/` | Traefik as API Gateway |
| ADR-002 | 🟡 Important | `docs/architecture/adr/` | Rust + Actix Services |
| ADR-003 | 🟡 Important | `docs/architecture/adr/` | Expo SDK 54 Lock |
| ADR-004 | 🟡 Important | `docs/architecture/adr/` | Clickstream in Admin Service |
| ADR-005 | 🟡 Important | `docs/architecture/adr/` | PostGIS Spatial Indexes |
| ADR-006 | 🟡 Important | `docs/architecture/adr/` | pnpm Only |
| ADR-007 | 🟢 Critical | `docs/architecture/adr/` | Source-Rooted Codebase |
| ADR-008 | 🟡 Important | `docs/architecture/adr/` | No ngrok Usage |
| ADR-009 | 🟡 Important | `docs/architecture/adr/` | Dark Mode from Day One |
| ADR-010 | 🟡 Important | `docs/architecture/adr/` | MapContainer Abstraction |

**Why Critical:** ADRs prevent the LLM from second-guessing decisions mid-session. They provide rationale and decision history.

---

### Schema Documentation

| File | Priority | Location | Description |
|------|----------|----------|-------------|
| `platform-db-schema.md` | 🟢 Critical | `docs/database/` | Human-readable schema reference with examples |

**Why Critical:** The LLM needs to know column names, types, and relationships before writing queries. This serves as the single source of truth for the schema.

---

### MVP Documentation

| File | Priority | Location | Description |
|------|----------|----------|-------------|
| `mvp-1-discovery-core.md` | 🟢 Critical | `docs/mvp/` | Scoped task list and scope definition |

**Why Critical:** Claude Code reads files, not widgets. This provides the exact scope, deliverables, and success criteria.

---

### Skills

| File | Priority | Location | Description |
|------|----------|----------|-------------|
| `uiux-pro-max/SKILL.md` | 🟢 Critical | `docs/skills/uiux-pro-max/` | UX Pro Max standards and enforcement rules |

**Why Critical:** This provides enforcement mechanism for Section 7 of the constitution. Without it, the constitution has no teeth.

---

## Priority Levels

### 🟢 Critical (Must Have - Phase 1)
1. Database schemas (001, 003, 004)
2. Docker Compose + .env.example
3. Design tokens + theme
4. Seed data
5. MVP task list
6. Human-readable schema
7. UI/UX Pro Max skill

### 🟡 Important (Should Have - Phase 2)
1. ADRs (7-10)
2. Original ADRs (1-6)

---

## Recommended Generation Order

```
Phase 1: Foundation (Days 1-2)
├── Database schemas (001, 003)
├── Docker Compose + .env.example
└── Core-files-importance.md

Phase 2: Design System (Days 3-4)
├── Design tokens + theme
├── Seed data
└── MVP task list

Phase 3: Documentation (Days 5-6)
├── Human-readable schema
├── ADRs (7-10)
├── UI/UX Pro Max skill
└── Core-files-summary.md
```

---

## LLM Rules

**Before generating any code, the LLM must:**
1. ✅ Check [`core-files-importance.md`](./core-files-importance.md) for reasoning
2. ✅ Check [`core-files-summary.md`](./core-files-summary.md) for quick reference
3. ✅ Verify required files exist
4. ✅ Read schema files if creating queries
5. ✅ Read ADRs before making architectural decisions
6. ✅ Read MVP task list before creating features

**If a file is missing from this list, the LLM must create it before proceeding.**

---

## Verification Checklist

- [ ] `infra/migrations/001_platform_db_init.sql` exists
- [ ] `infra/migrations/003_analytics_db_init.sql` exists
- [ ] `infra/migrations/004_seed_stations.sql` exists
- [ ] `infra/docker-compose.yml` exists
- [ ] `infra/.env.example` exists
- [ ] `source/mobile-driver/design/tokens.ts` exists
- [ ] `source/mobile-driver/design/theme.ts` exists
- [ ] `docs/architecture/adr/ADR-007-source-rooted-codebase.md` exists
- [ ] `docs/architecture/adr/ADR-008-no-ngrok.md` exists
- [ ] `docs/architecture/adr/ADR-009-dark-mode-from-day-one.md` exists
- [ ] `docs/architecture/adr/ADR-010-mapcontainer-abstraction.md` exists
- [ ] `docs/database/platform-db-schema.md` exists
- [ ] `docs/mvp/mvp-1-discovery-core.md` exists
- [ ] `docs/skills/uiux-pro-max/SKILL.md` exists
- [ ] `docs/core-files-importance.md` exists
- [ ] `docs/core-files-summary.md` exists

---

## Status Tracking

| File | Created | Status |
|------|---------|--------|
| `001_platform_db_init.sql` | ⏳ | Pending |
| `003_analytics_db_init.sql` | ⏳ | Pending |
| `004_seed_stations.sql` | ⏳ | Pending |
| `docker-compose.yml` | ⏳ | Pending |
| `.env.example` | ⏳ | Pending |
| `design/tokens.ts` | ⏳ | Pending |
| `design/theme.ts` | ⏳ | Pending |
| `ADR-007` | ✅ | Done |
| `ADR-008` | ✅ | Done |
| `ADR-009` | ✅ | Done |
| `ADR-010` | ✅ | Done |
| `platform-db-schema.md` | ✅ | Done |
| `mvp-1-discovery-core.md` | ✅ | Done |
| `uiux-pro-max/SKILL.md` | ✅ | Done |
| `core-files-importance.md` | ✅ | Done |
| `core-files-summary.md` | ✅ | Done |

---

## Related Documentation

- **Constitution:** [`docs/constitution-v1.0.md`](./constitution-v1.0.md) - Rules and principles
- **API Contract:** [`docs/api/api-contract.md`](./api/api-contract.md) - API definitions
- **Analytics Schema:** [`docs/database/analytics-db-schema.md`](./database/analytics-db-schema.md) - Analytics table definitions
- **Execution Log:** [`EXECUTION-LOG.md`](../EXECUTION-LOG.md) - Session tracking

---

**Last Updated:** 2026-06-11
**Status:** All documentation files created. Runtime files pending implementation.
