# BorneMap — Execution Log

**Project:** BorneMap — UX-first EV charging map platform  
**Constitution:** v1.0  
**Started:** 2026-06-10

---

## How to use this file

Update this file at the end of every Claude Code session.

- **Execution log** — one entry per session: what was built, what changed, what was skipped
- **Bug tracker** — one entry per bug found: status, root cause, fix
- **Decisions log** — any in-session decision that deviates from or extends the constitution
- **Blockers** — anything stopping the next session from starting

Keep entries in reverse chronological order (newest at top).

---

## Current Status

| Item | Status | Notes |
|------|--------|-------|
| Constitution v1.0 | ✅ Complete | [docs/constitution-v1.0.md](docs/constitution-v1.0.md) |
| CLAUDE.md | ✅ Complete | Session contract ready |
| API Contract v1.0 | ✅ Complete | [docs/api/api-contract.md](docs/api/api-contract.md) |
| Architecture docs | ✅ Complete | ADRs + C4 diagram in `docs/architecture/` |
| Database schemas | ✅ Complete | [docs/database/](docs/database/) |
| MVP-1 plan | ✅ Complete | [docs/mvp/mvp-1-discovery-core.md](docs/mvp/mvp-1-discovery-core.md) |
| MVP-2 through MVP-6 | ✅ Complete | Brief outlines in `docs/mvp/` |
| UI/UX Pro Max skill | ⏳ Pending | Awaiting user input |
| Tunisia seed data | ⏳ Pending | Generated in Session 1 (infra phase) |
| Docker Compose scaffold | ⏳ Pending | Session 1: Infra phase |
| platform_db migrations | ⏳ Pending | Session 1: Infra phase |
| Service implementations | ⏳ Pending | Session 2+: Backend phase |
| Mobile app scaffold | ⏳ Pending | Session 3+: Frontend phase |

---

## MVP-1 Progress (Discovery Core)

Track progress against [MVP-1 work breakdown](docs/mvp/mvp-1-discovery-core.md):

| Track | Task | Status | Session | Notes |
|-------|------|--------|---------|-------|
| **Infra** | Docker Compose scaffold | ⏳ | — | postgres:16 + postgis + traefik |
| **Infra** | platform_db initialization | ⏳ | — | schemas: inventory, gis, users |
| **Infra** | analytics_db initialization | ⏳ | — | raw_events table (append-only) |
| **Infra** | GIS schema + indexes | ⏳ | — | PostGIS spatial indexes for performance |
| **Infra** | Migration scripts | ⏳ | — | idempotent SQL files |
| **Infra** | .env.example | ⏳ | — | Document all variables |
| **Infra** | Verify DB connectivity | ⏳ | — | Test from Docker services |
| **Driver Svc** | Actix scaffold | ⏳ | — | sqlx, serde, tokio setup |
| **Driver Svc** | GET /api/v1/stations | ⏳ | — | Paginated list endpoint |
| **Driver Svc** | GET /api/v1/stations/nearby | ⏳ | — | Radius search endpoint |
| **Driver Svc** | GET /api/v1/stations/{id} | ⏳ | — | Station detail endpoint |
| **Driver Svc** | Error handling | ⏳ | — | Common error response shape |
| **Driver Svc** | Unit + contract tests | ⏳ | — | 80%+ coverage target |
| **Admin Svc** | Actix scaffold | ⏳ | — | Same as driver-service |
| **Admin Svc** | POST /api/v1/stations | ⏳ | — | Create station + chargers |
| **Admin Svc** | PUT /api/v1/stations/{id} | ⏳ | — | Partial update endpoint |
| **Admin Svc** | DELETE /api/v1/stations/{id} | ⏳ | — | Soft-delete endpoint |
| **Admin Svc** | POST /api/v1/events | ⏳ | — | Single event ingestion |
| **Admin Svc** | POST /api/v1/events/batch | ⏳ | — | Batch event ingestion |
| **Admin Svc** | Unit + contract tests | ⏳ | — | 80%+ coverage target |
| **Design Sys** | tokens.ts | ⏳ | — | Colors, spacing, typography (dark) |
| **Design Sys** | Skeleton components | ⏳ | — | SkeletonBox, SkeletonGroup, etc |
| **Design Sys** | Empty + error states | ⏳ | — | Fully designed (no blanks) |
| **Design Sys** | CTA + haptics | ⏳ | — | Primary button with expo-haptics |
| **Design Sys** | Dark mode setup | ⏳ | — | Light/dark token variants |
| **Mobile** | Expo SDK 54 scaffold | ⏳ | — | pnpm install, setup |
| **Mobile** | MapContainer abstraction | ⏳ | — | Single file for maps |
| **Mobile** | Station list screen | ⏳ | — | Paginated, skeleton loading |
| **Mobile** | Map screen | ⏳ | — | Markers, interactive |
| **Mobile** | Nearby search flow | ⏳ | — | Geolocation → query |
| **Mobile** | Station detail screen | ⏳ | — | Chargers, hours, map |
| **Mobile** | Bottom sheet modal | ⏳ | — | Swipe-to-dismiss gestures |
| **Mobile** | Dark mode toggle | ⏳ | — | Settings or quick action |
| **Mobile** | expo-router setup | ⏳ | — | File-based routing |
| **Mobile** | Zustand + React Query | ⏳ | — | State + data fetching |
| **Mobile** | Optimistic UI | ⏳ | — | Instant feedback on actions |
| **Mobile** | Error handling | ⏳ | — | Contextual recovery actions |
| **Integration** | Traefik config | ⏳ | — | Route services correctly |
| **Integration** | App → services wiring | ⏳ | — | API client setup |
| **Integration** | E2E tests | ⏳ | — | Full discovery flow |
| **Stabilization** | Performance optimization | ⏳ | — | <100ms queries, <5% battery |
| **Stabilization** | Device testing | ⏳ | — | iOS + Android real devices |
| **Stabilization** | Accessibility audit | ⏳ | — | Contrast, touch targets, etc |
| **Stabilization** | Launch prep | ⏳ | — | README, Postman collection, etc |

**Status legend:** ✅ done · 🔄 in progress · ❌ blocked · ⏳ pending

---

## Execution Log

### Session 000 — 2026-06-10 (Pre-session / Planning)

**Scope:** Documentation generation, architecture finalization

**Completed:**
- Constitution v1.0 finalized and written to `docs/constitution-v1.0.md`
- CLAUDE.md (Claude Code session contract) ready for implementation sessions
- API Contract v1.0 written to `docs/api/api-contract.md` (all endpoints documented)
- Architecture diagram generated (`docs/architecture/bornemaps-architecture.mermaid`)
- All 6 ADRs written (`docs/architecture/adr/ADR-001 through ADR-006.md`)
- Database schema documentation complete:
  - `docs/database/platform-db-schema.md` (inventory, gis, users)
  - `docs/database/analytics-db-schema.md` (append-only events)
- MVP-1 discovery core plan written (`docs/mvp/mvp-1-discovery-core.md`)
- MVP-2 through MVP-6 brief outlines created
- This execution log initialized

**Decisions made:**
- Traefik promoted to API gateway (not direct service access)
- Clickstream service dropped — events live in admin-service
- Expo SDK locked at 54 indefinitely
- Monorepo root: `source/` (not `src/`)
- Two services only: driver-service (:8080), admin-service (:8081)
- pnpm as sole package manager (no npm, no yarn)
- PostGIS spatial indexes for geospatial performance

**Deviations from constitution:** None (all decisions now have ADRs)

**Skipped / deferred:**
- UI/UX Pro Max skill file generation (awaiting user request)
- Tunisia seed data (generated in next session during infra phase)
- Service implementations (deferred to Session 1+)
- Mobile app scaffold (deferred to Session 2+)

**Next session starts at:** Session 1 — Infra + Database (Docker Compose, migrations, test data)

---

## Bug Tracker

### Open Bugs
None yet.

### Closed Bugs
None yet.

### Bug Template

```markdown
### BUG-NNN — short title
**Found:** YYYY-MM-DD · Session NNN  
**Status:** open | investigating | fixed | wont-fix  
**Severity:** critical | high | medium | low  
**Track:** infra | driver-service | admin-service | mobile | database | integration  

**Description:**
What went wrong.

**Root cause:**
Why it happened.

**Fix:**
What was done to resolve it.

**Commit / file:**
```

---

## Decisions Log

Decisions that extend or deviate from the constitution. Each eventually becomes an ADR in `docs/architecture/adr/`.

| # | Date | Decision | Reason | ADR |
|---|------|----------|--------|-----|
| D001 | 2026-06-10 | Traefik as API gateway | Simplifies TLS, routing, auth in one place | ADR-001 |
| D002 | 2026-06-10 | Clickstream service dropped | Only used by dashboard; no dedicated service needed | ADR-004 |
| D003 | 2026-06-10 | Expo SDK locked at 54 | Stability; no unplanned regressions | ADR-003 |
| D004 | 2026-06-10 | source/ as monorepo root (not src/) | Clearer separation of runtime vs. non-runtime | N/A (Constitutional) |
| D005 | 2026-06-10 | Two services only (driver + admin) | Clickstream merged into admin-service | ADR-004 |
| D006 | 2026-06-10 | pnpm only (no npm/yarn) | Speed, disk efficiency, determinism | ADR-006 |
| D007 | 2026-06-10 | PostGIS spatial indexes | Critical for <100ms nearby search | ADR-005 |

---

## Blockers

| # | Blocker | Blocks | Since | Status |
|---|---------|--------|-------|--------|
| B-001 | UI/UX Pro Max skill file not generated | Section 7 UX rules incomplete | 2026-06-10 | 🔄 Awaiting user request |
| B-002 | Tunisia seed data not generated | MVP-1 map testing (Day 1) | 2026-06-10 | ⏳ Next session (Infra phase) |

---

## Known Constitutional Violations to Watch

**Things Claude Code is likely to get wrong based on common mistakes:**

| Risk | Rule | Watch For | Fix |
|------|------|-----------|-----|
| Hardcoded colors | Rule 11 (tokens discipline) | Hex values outside `tokens.ts` | All colors → tokens.ts |
| pnpm lockfile errors | Rule 13 (pnpm only) | `ERR_PNPM_MINIMUM_RELEASE_AGE_VIOLATION` | `pnpm install --no-frozen-lockfile` |
| Files created outside source/ | Rule 1 (source-rooted) | .ts/.tsx/.rs outside `source/` | Verify path before write |
| Platform.OS outside MapContainer | Rule 18 | `Platform.OS` checks elsewhere | Only in `MapContainer.tsx` |
| Core Animated API usage | Rule 10 (reanimated v3 only) | `import { Animated }` from react-native | Use reanimated v3 only |
| Clickstream service created | Rule 17 | New folder `source/clickstream-service/` | Never create; events in admin-service |

---

## Session Statistics

| Session | Date | Duration | Lines Added | Components Built | Tests Written |
|---------|------|----------|-------------|------------------|---------------|
| 000 | 2026-06-10 | 2.5h | ~4,000 | 0 | 0 |
| 001 | ⏳ | — | — | — | — |
| 002 | ⏳ | — | — | — | — |

---

## References & Quick Links

**Constitution & Contracts:**
- [Constitution v1.0](docs/constitution-v1.0.md)
- [API Contract v1.0](docs/api/api-contract.md)

**Architecture:**
- [Mermaid Diagram](docs/architecture/bornemaps-architecture.mermaid)
- [ADR-001: Traefik](docs/architecture/adr/ADR-001-traefik-as-gateway.md)
- [ADR-002: Rust/Actix](docs/architecture/adr/ADR-002-rust-actix-services.md)
- [ADR-003: Expo SDK 54](docs/architecture/adr/ADR-003-expo-sdk-54-lock.md)
- [ADR-004: Clickstream in Admin](docs/architecture/adr/ADR-004-clickstream-in-admin-service.md)
- [ADR-005: PostGIS Indexes](docs/architecture/adr/ADR-005-postgis-spatial-index.md)
- [ADR-006: pnpm Only](docs/architecture/adr/ADR-006-pnpm-only.md)

**Database:**
- [platform_db Schema](docs/database/platform-db-schema.md)
- [analytics_db Schema](docs/database/analytics-db-schema.md)

**MVP Planning:**
- [MVP-1: Discovery Core](docs/mvp/mvp-1-discovery-core.md)
- [MVP-2: Operational Control](docs/mvp/mvp-2-operational.md)
- [MVP-3: Identity & RBAC](docs/mvp/mvp-3-identity.md)

---

**Last Updated:** 2026-06-10  
**Next Session:** Session 1 — Infra + Database
