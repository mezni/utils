# BorneMap Core Files - Quick Reference

## What This Document Is

This is a summary of the 10 critical files that must exist to prevent the LLM from making inconsistent decisions in the BorneMap project. These files serve as the "rulebook" for code generation.

---

## The 10 Core Files

### 1. Database Schema Files

#### `infra/migrations/001_platform_db_init.sql`
**What it contains:**
- Partner, station, charger tables with PostGIS geometry
- Entity-prefixed IDs (STA-, CHR-, PRT-)
- Soft delete pattern
- Spatial indexes for performance

**Why LLM needs it:**
- Without this, LLM invents column names like `station_name`, `charger_status`
- Prevents invalid SQL queries
- Enforces consistent schema across all code generation

---

#### `infra/migrations/003_analytics_db_init.sql`
**What it contains:**
- Raw events table with append-only constraints
- Event types, metadata, timestamps
- Triggers to prevent UPDATE/DELETE

**Why LLM needs it:**
- Without this, LLM invents event schemas
- Ensures event audit trail
- Prevents data modification errors

---

#### `infra/migrations/004_seed_stations.sql`
**What it contains:**
- Tunisia seed data with real coordinates
- Partners, stations, chargers
- Multiple connector types

**Why LLM needs it:**
- Without real coordinates, map screen can't be tested
- Provides baseline data for QA
- Ensures map functionality works

---

### 2. Infrastructure Files

#### `infra/docker-compose.yml`
**What it contains:**
- Service configuration (driver, admin, postgres)
- Port mappings (8080, 8081)
- Network isolation
- Volume definitions

**Why LLM needs it:**
- Without this, LLM invents port mappings, volume names
- Prevents port conflicts
- Ensures consistent service discovery

---

#### `infra/.env.example`
**What it contains:**
- Database URL
- Service ports
- JWT settings
- CORS configuration

**Why LLM needs it:**
- LLM won't know what to inject without this registry
- Prevents runtime errors
- Documents all required environment variables

---

### 3. Design System Files

#### `source/front/mobile-driver/design/tokens.ts`
**What it contains:**
- Colors (light and dark variants)
- Spacing scale
- Typography scale
- Border radius
- Shadows

**Why LLM needs it:**
- The LLM will hardcode values the moment this file doesn't exist
- Ensures consistent design language
- Enforces Rule 11 (no hardcoded values)

---

#### `source/front/mobile-driver/design/theme.ts`
**What it contains:**
- Light theme object
- Dark theme object
- Theme switching logic
- Global theme context

**Why LLM needs it:**
- Without it, every component reimplements theming differently
- Ensures theme switching works globally
- Prevents theme inconsistencies

---

### 4. Documentation Files

#### `docs/architecture/adr/` (4 ADRs)
**What they contain:**
- ADR-001: Traefik as gateway
- ADR-003: Expo SDK 54 lock
- ADR-004: Clickstream in admin-service
- ADR-007: Source-rooted codebase

**Why LLM needs them:**
- Without ADRs, LLM will second-guess decisions mid-session
- Prevents contradictory decisions
- Provides rationale for long-term consistency

---

#### `docs/database/platform-db-schema.md`
**What it contains:**
- Human-readable table definitions
- Column metadata (type, nullable, notes)
- Query examples
- Index and constraint documentation

**Why LLM needs it:**
- LLM needs to know column names, types, relationships before writing queries
- Serves as system of record for schema evolution
- Prevents query generation errors

---

#### `docs/mvp/mvp-1-discovery-core.md`
**What it contains:**
- Scope and deliverables
- Work breakdown by phase
- Definition of Done
- Success criteria
- Risk assessments

**Why LLM needs it:**
- Claude Code reads files, not widgets
- Provides scoped task list
- Ensures correct feature scope

---

#### `docs/skills/uiux-pro-max/SKILL.md`
**What it contains:**
- Skeleton screens over spinners
- Optimistic UI on actions
- Haptic feedback rules
- Design token discipline
- Quality standards

**Why LLM needs it:**
- Enforces Section 7 of the constitution
- Defines exact implementation standards
- Provides testing checklist

---

## Recommended Generation Order

```
Phase 1: Foundation (Days 1-2)
├── 1. 001_platform_db_init.sql (stations + chargers DDL)
├── 2. 003_analytics_db_init.sql (raw_events DDL)
├── 3. docker-compose.yml (local infra contract)
└── 4. .env.example (env var registry)

Phase 2: Design System (Days 3-4)
├── 5. source/front/mobile-driver/design/tokens.ts
├── 6. source/front/mobile-driver/design/theme.ts
├── 7. 004_seed_stations.sql (Tunisia seed data)
└── 8. docs/mvp/mvp-1-discovery-core.md (task list)

Phase 3: Documentation (Days 5-6)
├── 9. docs/database/platform-db-schema.md
└── 10. docs/architecture/adr/ (4 ADRs)
```

---

## Priority Levels

### Critical (Must Have - Phase 1)
1. Database schemas (001, 003)
2. Docker Compose + .env.example

### Important (Should Have - Phase 2)
3. Design tokens + theme
4. Seed data

### Nice to Have (Enhances Quality - Phase 3)
5. MVP task list
6. Human-readable schema
7. ADRs
8. UX Pro Max skill

---

## LLM Guidance

**Before generating any code, LLM must check these files:**
1. ✅ Database schema files exist → Can generate correct queries
2. ✅ Docker Compose exists → Can generate correct service configs
3. ✅ Tokens exist → Will use tokens instead of hardcoded values
4. ✅ Seed data exists → Can test map functionality
5. ✅ ADRs exist → Won't second-guess architectural decisions
6. ✅ Human-readable schema exists → Will generate correct queries
7. ✅ MVP task list exists → Won't invent extra features
8. ✅ Theme exists → Won't reimplement theming per component
9. ✅ Env example exists → Will know what to inject
10. ✅ UX Pro Max skill exists → Won't violate constitutional rules

**If any file is missing, LLM must create it before proceeding with code generation.**

---

## Why These Files Matter

### Preventing LLM Inconsistency
- **Database Schema** → LLM invents columns without DDL
- **Docker Compose** → LLM invents ports, volumes, env vars
- **Design Tokens** → LLM hardcodes values without source of truth
- **Seed Data** → LLM can't test map without real coordinates
- **ADRs** → LLM second-guesses architectural decisions
- **Human-Readable Schema** → LLM generates incorrect queries
- **MVP Task List** → LLM invents tasks without scope boundaries
- **Theme File** → LLM reimplements theming differently per component
- **Env Example** → LLM misses required environment variables
- **UX Pro Max Skill** → LLM violates constitutional rules

### Ensuring Quality
These files collectively ensure:
- ✅ **Correctness:** Valid database schemas, correct queries
- ✅ **Consistency:** Uniform design system, consistent themes
- ✅ **Testability:** Real data for QA, reproducible environments
- ✅ **Maintainability:** Clear decision history, documented patterns
- ✅ **Quality:** UX standards enforced, constitutional compliance

---

## Summary

**The 10 core files form the foundation of the BorneMap project.** Without them, the LLM will make incorrect, inconsistent, or incomplete decisions. With them, the LLM has a clear rulebook to follow, ensuring high-quality, consistent, and testable code.

**Rule:** "If a file is missing from this list, the LLM must create it before generating any code."
