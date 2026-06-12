# MVP-1: Discovery Core

**Status:** Planning → In Progress → Stabilization  
**Timeline:** 4-6 weeks  
**Goal:** Launch a high-performance map-based station discovery experience for drivers

---

## Core Files for LLM Consistency

**Critical files that must exist to prevent LLM from making inconsistent decisions:**

| File | Purpose | Why Critical |
|------|---------|--------------|
| `infra/migrations/001_platform_db_init.sql` | Stations + chargers DDL with PostGIS geometry | Without this, LLM invents column names |
| `infra/migrations/003_analytics_db_init.sql` | Raw_events table with append-only constraints | Prevents event modification errors |
| `infra/docker-compose.yml` | Local infra contract (ports, volumes, services) | Without this, LLM invents config values |
| `infra/.env.example` | Environment variable registry | LLM won't know what to inject |
| `source/mobile-driver/design/tokens.ts` | Design system foundation | LLM hardcodes values without this |
| `source/mobile-driver/design/theme.ts` | Dark/light theme object | Prevents per-component theming |
| `infra/migrations/004_seed_stations.sql` | Tunisia seed data with real coordinates | Map screen can't be tested otherwise |
| `docs/architecture/adr/` (×4) | Decision records | Prevents LLM second-guessing decisions |
| `docs/database/platform-db-schema.md` | Human-readable schema reference | LLM needs to know column names/types |
| `docs/mvp/mvp-1-discovery-core.md` | Scoped task list document | Claude Code reads files, not widgets |

**Recommended Generation Order:**
1. Database schemas (001, 003, 004)
2. Docker Compose + .env.example
3. Design tokens + theme
4. MVP task list
5. ADRs
6. Documentation (human-readable schema)

**See [`docs/core-files-importance.md`](./core-files-importance.md) for detailed reasoning.**

---

## Scope

MVP-1 delivers the minimal viable product for **drivers to find charging stations** through a map interface:

1. **Station discovery** — list and map view of all stations
2. **Geospatial search** — find nearby stations (radius search)
3. **Station detail** — view chargers, pricing, hours
4. **Event instrumentation** — capture user interactions for future analytics
5. **Dark mode** — support both light and dark themes from day one
6. **Mobile-first UX** — skeleton screens, optimistic UI, haptics, bottom sheets

### Out of Scope (MVP-2+)
- Partner management UI
- Admin dashboard
- User authentication (Keycloak integration)
- Real-time charger availability updates
- Booking/reservation system
- Payment processing

---

## Architecture Decisions

All final architectural decisions are documented in `docs/architecture/adr/`:

| ADR | Decision | Rationale |
|-----|----------|-----------|
| ADR-001 | Traefik as API gateway | Simplifies routing, TLS, auth middleware |
| ADR-002 | Rust + Actix for services | Low latency, high throughput, type safety |
| ADR-003 | Expo SDK 54 locked | Stability, no unplanned regressions |
| ADR-004 | Clickstream in admin-service | No dedicated service needed for MVP |
| ADR-005 | PostGIS spatial indexes | Critical for <100ms geospatial queries |
| ADR-006 | pnpm only | Speed, disk efficiency, determinism |

---

## Work Breakdown

### Phase 1: Infrastructure & Database (Week 1)

**Goal:** Set up local development environment with running database.

| Task | Owner | Status | Notes |
|------|-------|--------|-------|
| Docker Compose scaffold | Claude Code | ⏳ | postgres:16, postgis, pgadmin |
| platform_db initialization | Claude Code | ⏳ | Create database, schemas, tables |
| analytics_db initialization | Claude Code | ⏳ | Create raw_events table (append-only) |
| GIS schema + PostGIS indexes | Claude Code | ⏳ | Load OSM boundaries, create spatial indexes |
| Migration scripts | Claude Code | ⏳ | idempotent SQL in `infra/migrations/` |
| .env.example | Claude Code | ⏳ | Document all environment variables |
| Verify DB connectivity | Claude Code | ⏳ | Test connections from Docker services |

**Deliverables:**
- Running PostgreSQL + PostGIS
- Three initialized schemas (inventory, gis, users)
- All indexes created
- Test data seed (Tunisia stations)

---

### Phase 2: Backend Services (Week 2-3)

**Goal:** Build two Rust services with all API endpoints.

#### Driver Service (:8080)

| Task | Owner | Status | Notes |
|------|-------|--------|-------|
| Actix scaffold + dependencies | Claude Code | ⏳ | sqlx, serde, tokio, uuid |
| Database connection pool | Claude Code | ⏳ | sqlx with `platform_db` |
| `GET /api/v1/stations` | Claude Code | ⏳ | Paginated list (page, per_page) |
| `GET /api/v1/stations/nearby` | Claude Code | ⏳ | ST_DWithin radius query |
| `GET /api/v1/stations/{id}` | Claude Code | ⏳ | Full detail + chargers array |
| Error handling | Claude Code | ⏳ | Common error response shape |
| Health check endpoint | Claude Code | ⏳ | `GET /health` for monitoring |
| Logging + observability | Claude Code | ⏳ | env_logger, request IDs |

**Test Coverage:**
- Unit tests for each handler
- Integration tests with test database
- Contract tests against API spec

#### Admin Service (:8081)

| Task | Owner | Status | Notes |
|------|-------|--------|-------|
| Actix scaffold | Claude Code | ⏳ | Same as driver-service |
| Database connection pool | Claude Code | ⏳ | `platform_db` (inventory schema) |
| `POST /api/v1/stations` | Claude Code | ⏳ | Create station + chargers |
| `PUT /api/v1/stations/{id}` | Claude Code | ⏳ | Partial update |
| `DELETE /api/v1/stations/{id}` | Claude Code | ⏳ | Soft-delete (deleted_at) |
| `POST /api/v1/events` | Claude Code | ⏳ | Single event → analytics_db |
| `POST /api/v1/events/batch` | Claude Code | ⏳ | Up to 100 events, batched write |
| Error handling + validation | Claude Code | ⏳ | Validate all inputs |

**Test Coverage:**
- Unit tests
- Integration tests
- Event batching stress tests (1000+ events)

**Deliverables:**
- Two compiled Rust binaries
- All endpoints responding with correct JSON
- Error cases handled (404, 400, 500)
- Logs available for debugging

---

### Phase 3: Design System & Components (Week 2-3)

**Goal:** Build reusable UI components for mobile, following UX Pro Max standard.

| Task | Owner | Status | Notes |
|------|-------|--------|-------|
| Design tokens (tokens.ts) | Claude Code | ⏳ | Colors, spacing, typography (dark mode) |
| SkeletonBox component | Claude Code | ⏳ | Animated skeleton placeholder |
| SkeletonGroup component | Claude Code | ⏳ | Multiple skeletons (list, detail) |
| StationListItemSkeleton | Claude Code | ⏳ | Skeleton for station list row |
| StationDetailSkeleton | Claude Code | ⏳ | Skeleton for detail view |
| SearchBarSkeleton | Claude Code | ⏳ | Skeleton for search input |
| EmptyState component | Claude Code | ⏳ | Fully designed empty screen |
| ErrorState component | Claude Code | ⏳ | Error + recovery action button |
| CTA button + haptics | Claude Code | ⏳ | Primary button with expo-haptics |
| Dark mode setup | Claude Code | ⏳ | Light/dark token variants |
| reanimated v3 setup | Claude Code | ⏳ | Animation provider, utility hooks |

**Requirements:**
- No hardcoded colors/spacing outside tokens.ts
- Dark mode toggle-able from day one
- All components accept dark mode via theme prop
- Haptic feedback on all primary actions

**Deliverables:**
- Reusable component library
- Storybook or similar for dev preview
- Dark mode tested on real device

---

### Phase 4: Mobile App (Week 3-4)

**Goal:** Build driver-facing map + detail experience.

| Task | Owner | Status | Notes |
|------|-------|--------|-------|
| Expo SDK 54 project scaffold | Claude Code | ⏳ | pnpm install, .env setup |
| MapContainer abstraction | Claude Code | ⏳ | Single file for react-native-maps + leaflet |
| Station list screen | Claude Code | ⏳ | Paginated list, skeleton loading |
| Map screen | Claude Code | ⏳ | Markers, cluster (future) |
| Nearby search flow | Claude Code | ⏳ | Geolocation → radius query → map |
| Station detail screen | Claude Code | ⏳ | Chargers, hours, map location |
| Bottom sheet modal | Claude Code | ⏳ | Station preview (swipe to detail) |
| Dark mode toggle | Claude Code | ⏳ | Settings screen or quick toggle |
| expo-router v3 setup | Claude Code | ⏳ | File-based routing, deep links |
| Zustand store (UI state) | Claude Code | ⏳ | Dark mode, search filters, map state |
| React Query client | Claude Code | ⏳ | API data fetching + caching |
| Optimistic UI | Claude Code | ⏳ | Favorite/unfavorite before response |
| Error handling + retry | Claude Code | ⏳ | Contextual recovery actions |

**UX Requirements:**
- Skeleton screens, never spinners
- Haptic feedback on all CTAs
- Bottom sheet gestures (swipe-to-dismiss)
- Pull-to-refresh on list/map
- Dark mode works perfectly
- No map jitter or marker flashing
- Route transitions smooth (expo-router animations)

**Deliverables:**
- Running mobile app
- Can search nearby stations
- Can view station detail + chargers
- Dark mode working
- All skeletons, empty states, errors designed

---

### Phase 5: Integration & Testing (Week 4-5)

**Goal:** Wire up all three components, run end-to-end tests.

| Task | Owner | Status | Notes |
|------|-------|--------|-------|
| Traefik configuration | Claude Code | ⏳ | Route `/api/v1/*` to services |
| App → driver-service wiring | Claude Code | ⏳ | API client pointing to traefik |
| Nearby search e2e test | Claude Code | ⏳ | Geolocation → query → map |
| Station detail e2e test | Claude Code | ⏳ | Tap marker → detail view |
| Event logging e2e | Claude Code | ⏳ | Events captured, written to analytics_db |
| Dark mode e2e | Claude Code | ⏳ | Toggle works, colors correct |
| Error handling e2e | Claude Code | ⏳ | Network error → recovery action |
| Contract tests (driver-service) | Claude Code | ⏳ | Validate API responses |
| Contract tests (admin-service) | Claude Code | ⏳ | Validate API responses |

**Test Coverage Target:**
- 80%+ backend unit test coverage
- 100% critical path contract tests
- Manual E2E on real device (iOS + Android)

**Deliverables:**
- All services talking to each other
- Full discovery flow working locally
- Test suite passing
- Load test: 1000+ nearby searches <100ms

---

### Phase 6: Stabilization Sprint (Week 5-6)

**Goal:** Polish, optimize, audit UX.

| Task | Owner | Status | Notes |
|------|-------|--------|-------|
| Map jitter fix | Claude Code | ⏳ | Profile, optimize re-renders |
| PostGIS query latency | Claude Code | ⏳ | Benchmark radius queries |
| API payload reduction | Claude Code | ⏳ | Minimize JSON size (strip nulls) |
| Skeleton + transition polish | Claude Code | ⏳ | Loading state animations smooth |
| Event consistency E2E | Claude Code | ⏳ | All interactions logged correctly |
| Error state audit | Claude Code | ⏳ | Every error path has recovery |
| Empty state audit | Claude Code | ⏳ | No blank screens |
| Dark mode full audit | Claude Code | ⏳ | Every screen tested in dark |
| Accessibility audit | Claude Code | ⏳ | Text contrast, button sizes, touch targets |
| Performance profiling | Claude Code | ⏳ | Memory, CPU, battery impact |
| Device testing | Claude Code | ⏳ | iPhone 12/13/14+, Android 10+ |
| Crash reporting setup | Claude Code | ⏳ | Sentry or similar (optional for MVP) |

**Acceptance Criteria:**
- All interactions complete in <300ms
- Map renders 1000+ stations without jitter
- No console errors or warnings
- Battery impact <5% per 1hr usage
- Dark mode perfect on all screens
- Error recovery works on all paths
- Skeleton screens smooth (no flickering)

**Deliverables:**
- Optimized binary (mobile app size <100MB)
- Performance report
- UX audit checklist passed
- Ready for MVP-1 launch

---

## Sprint Structure

```
Week 1:    Infra + DB                 (4 days work)
Week 2:    Backend (Driver + Admin)   (5 days work)
Week 3:    Mobile app + Design system (5 days work)
Week 4:    Integration + Tests        (5 days work)
Week 5:    Stabilization sprint       (5 days work)
Week 6:    Buffer + launch prep       (3 days work)
```

---

## Definition of Done (MVP-1 Launch)

- [ ] All 30+ backend endpoints implemented and tested
- [ ] Mobile app runs on iOS and Android without crashes
- [ ] All discovery flows work end-to-end (list → map → detail → events)
- [ ] Dark mode works perfectly on all screens
- [ ] All UX Pro Max rules followed (skeletons, optimistic UI, haptics, gestures)
- [ ] 80%+ unit test coverage on backend
- [ ] 100% contract test coverage on API
- [ ] No console errors or warnings
- [ ] Postman/curl collection for manual API testing
- [ ] Docker Compose runs locally with one command
- [ ] README with setup instructions
- [ ] All docs in `docs/` complete

---

## Known Risks & Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| PostGIS query latency | Medium | High | Early benchmark (Week 1), optimize indexes if needed |
| Map render performance | Medium | High | Profile early (Week 3), use reanimated v3 correctly |
| Rust learning curve | Low | Medium | Single agent (Claude Code) removes coordination overhead |
| pnpm lockfile issues | Low | Low | Document ERR_PNPM_MINIMUM_RELEASE_AGE_VIOLATION fix |
| iOS/Android native issues | Low | High | Test on real devices weekly |
| Event batching bottleneck | Low | Low | Async processing, batch queue (MVP-2 if needed) |

---

## Success Metrics

**Performance:**
- Nearby search: <100ms (p95)
- Station detail: <50ms (p95)
- List pagination: <200ms (p95)
- Map with 1000+ markers: no jitter

**UX:**
- 100% skeleton screens (no spinners)
- All CTAs have haptic feedback
- Dark mode works on all screens
- Zero blank empty states
- All errors show recovery action

**Quality:**
- 0 crash rate on real devices
- 100% API contract test pass
- <5% battery drain per hour
- All accessibility checks pass

---

## Success Criteria Checklist

- [ ] `docker-compose up` starts all services
- [ ] Mobile app installs and runs without crashes
- [ ] `GET /api/v1/stations/nearby?lat=36.8&lng=10.1&radius=5` returns <100ms
- [ ] Dark mode toggle works smoothly
- [ ] All skeleton screens animate
- [ ] Bottom sheet swipe-to-dismiss works
- [ ] Network error shows contextual recovery button
- [ ] Empty state (no stations found) is fully designed
- [ ] All haptic feedback triggers on CTAs
- [ ] No `Platform.OS` outside MapContainer.tsx
- [ ] All design tokens from `tokens.ts` (no hardcoding)
- [ ] Postman collection has 10+ successful requests
- [ ] README complete with setup steps

---

## Next Steps (Post-MVP-1)

Once MVP-1 stabilizes and launches:

1. **Collect feedback** from beta testers (actual drivers)
2. **Monitor analytics** — which features are used, where are the drop-offs?
3. **Plan MVP-2** — partner management, admin dashboard
4. **Scale infra** — if needed (CDN, database replication, caching)
