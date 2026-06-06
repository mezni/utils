# BorneMap Implementation Plan

**Delivery Philosophy:** Each phase produces something visible and testable before the next phase begins. Phases build in layers — UI first, then data, then services, then connection, then infrastructure. Nothing is thrown away between phases. Mock data is replaced by real data, not rebuilt. Every phase ends with a hardening sprint that must pass before the next phase starts.

---

## Phase Map

| Phase | Goal | Key Deliverable |
|-------|------|---|
| 1 | Four Apps with Mock Data | Navigable UI, design system, RTL verified |
| 2 | Database Foundation | All schemas, OSM data, seeds |
| 3 | Backend Services | Three services running, all endpoints tested |
| 4 | Authentication & User Management | Keycloak, JWT middleware, auth flows |
| 5 | Connect Apps to Services | Real data in all apps, no mock data |
| 6 | GIS Synchronization | Trigger-based sync, spatial queries |
| 7 | Clickstream Analytics | Events tracked, aggregates, reporting |
| 8 | Traefik Production Runtime | TLS, routing, production Compose |
| 9 | Features & Launch Readiness | Reviews, favorites, profile, hardened |

---

## Phase 1 — Four Apps with Mock Data

**Goal:** All three frontend applications are fully navigable with realistic mock data. No backend. No auth. No network calls. Pure UI.

**Why first:** Validates the design system, component library, and screen inventory before any backend work begins. Bugs caught here cost nothing. Bugs caught in Phase 5 after services are built cost much more.

**Duration:** 10 weeks (5 sprints × 2 weeks each)

### Sprint 1.1 — Design System Foundation (2 weeks)

**Goal:** Design token package and core shared components are built, tested, and consumable by all apps.

**Tasks:**

**Setup:**
- Initialize monorepo with directory structure from docs/ codebase document
- Set up Cargo workspace root `Cargo.toml`
- Set up pnpm workspace `pnpm-workspace.yaml`
- Configure `tsconfig.base.json` shared TypeScript config
- Configure `.eslintrc.base.js` and `.prettierrc`

**packages/ui token foundation:**
- Create `src/tokens/colors.ts` with full color token set from constitution section 5.1
- Create `src/tokens/typography.ts` with font families and scale
- Create `src/tokens/spacing.ts` with 4px base unit scale
- Create `src/tokens/radius.ts` with full radius scale
- Create `src/tokens/shadows.ts` with card, panel, float, pin shadows
- Create `src/tokens/index.ts` re-exporting everything
- Create `src/tokens/native.ts` with React Native compatible values
- Write `tailwind.config.base.js` extending all tokens

**Shared components (web):**
- Button — primary, secondary, ghost, danger variants; sm, md, lg sizes; default, hover, active, disabled, loading states
- Input — text, password, search variants; default, focused, error, disabled states
- Badge — generic label component, configurable color
- StatusBadge — available, in-use, maintenance variants with colored dot
- Skeleton — block and text variants, animated pulse
- EmptyState — icon, title, description, optional action button
- ErrorState — icon, title, description, retry button
- Toast — success, error, warning, info variants, auto-dismiss
- Modal — sm, md, lg sizes; header, body, footer slots
- Table — sortable columns, pagination, row actions slot
- StatCard — label, value, optional trend indicator
- DataCard — title, optional action, body slot

Each component:
- Implemented in `src/components/ComponentName/ComponentName.tsx`
- Unit tested in `src/components/ComponentName/ComponentName.test.tsx`
- Exported from `src/components/index.ts`
- Documented entry added to `docs/ui/components.md`

**Done when:**
- ✅ `pnpm build` passes for `packages/ui`
- ✅ All component tests pass
- ✅ Tailwind config correctly resolves all token values
- ✅ `packages/ui/src/tokens/native.ts` exports all values in RN-compatible format
- ✅ `docs/ui/components.md` reflects every built component

### Sprint 1.2 — Driver Web App with Mock Data (2 weeks)

**Goal:** Driver Web App is fully navigable with all public screens populated from realistic mock data.

**Tasks:**

**App scaffold:**
- Initialize `apps/driver-web` with Vite + React + TypeScript
- Configure Tailwind extending `packages/ui/tailwind.config.base.js`
- Configure i18n — Arabic and French with RTL switching
- Set up router with all routes declared
- Set up mock data files

**Mock data (src/mocks/):**
- `stations.ts` — 15 stations with real Tunisian addresses and coordinates
- `chargers.ts` — 2–4 chargers per station with realistic connector types and power
- `reviews.ts` — 3–5 reviews per station with Arabic and French content

**Driver-specific components (in apps/driver-web/src/components/):**
- MobileTopBar (web variant) — menu icon, brand name, notification bell
- SearchBar — search icon, input, floating card style
- FilterPills — horizontal pill row, active/inactive states
- MapPinMarker — circle marker, default/selected/unavailable states, glow shadow
- ZoomControls — +/- button group
- StationCard — name, address, distance, charger count, availability badge
- ChargerRow — connector type, power kw, status badge
- ReviewCard — rating stars, date, text content
- BottomStationCard (web sidebar variant) — station summary with spec rows

**Screens:**
- Home/Map: full-bleed map placeholder (#EAF0E6 background), mock markers as positioned divs, SearchBar, FilterPills, StationCard list in sidebar, ZoomControls
- Station Detail: station info, ChargerRow list, ReviewCard list, rating summary
- Search Results: SearchBar, FilterPills, paginated StationCard list, EmptyState when no results
- Favorites: StationCard list, EmptyState
- Profile: form layout with Input fields, Button — static, no submission
- Login/Register: centered card, Input fields, social login buttons — static

**i18n:**
- All static strings translated in `ar.json` and `fr.json`
- RTL layout switching on all six screens verified in Arabic

**Done when:**
- ✅ All six screens render with mock data
- ✅ Navigation between all screens works
- ✅ Arabic RTL layout correct on every screen
- ✅ French layout correct on every screen
- ✅ No backend calls anywhere

### Sprint 1.3 — Driver Mobile App with Mock Data (2 weeks)

**Goal:** Driver Mobile App covers same screens as web with full-bleed map layout and bottom sheet pattern.

**Tasks:**

**App scaffold:**
- Initialize `apps/driver-mobile` with Expo + React Native + TypeScript
- Configure navigation: bottom tab navigator + stack navigator
- Set up i18n — Arabic and French with RTL switching via React Native RTL support
- Import native tokens from `packages/ui/src/tokens/native.ts`
- Set up mock data files (same shape as web mock data)

**Mobile-specific components:**
- MobileShell — full-bleed layout wrapper
- MobileTopBar — header with safe area top inset
- SearchBar (native) — using TextInput with native token styles
- FilterPills (native) — ScrollView horizontal pill row
- MapPinMarker — View with glow shadow via shadow.pin token
- BottomStationCard — absolute positioned bottom card with shadow.float
- SpecRow — detail row with label and value
- BottomTabBar — with safe area bottom inset
- CenterActionButton — raised circular button, gradient background
- ZoomControls — floating button group

**Screens:**
- Map/Home: full-bleed #EAF0E6 View, mock markers as absolutely positioned Views, SearchBar, FilterPills, BottomStationCard showing first mock station
- Station List: FlatList of StationCard, pull-to-refresh (no-op on mock), Skeleton on first load
- Station Detail: ScrollView, station info, FlatList of ChargerRow, FlatList of ReviewCard
- Search: TextInput with debounce, FilterPills, FlatList of results, EmptyState
- Favorites: FlatList of StationCard, EmptyState
- Profile: ScrollView of Input fields — static
- Login/Register: full screen form — static

**i18n and RTL:**
- All strings in `ar.json` and `fr.json`
- RTL layout tested on iOS simulator and Android emulator
- MobileTopBar, SearchBar, FilterPills, BottomStationCard RTL verified

**Done when:**
- ✅ All seven screens run on iOS simulator and Android emulator
- ✅ Navigation between all screens works
- ✅ Mock data displayed correctly
- ✅ Arabic RTL layout correct on every screen
- ✅ No backend calls anywhere

### Sprint 1.4 — Dashboard App with Mock Data (2 weeks)

**Goal:** Dashboard App is fully navigable in both partner and admin modes with mock data.

**Tasks:**

**App scaffold:**
- Initialize `apps/dashboard` with Vite + React + TypeScript
- Configure Tailwind extending `packages/ui/tailwind.config.base.js`
- Configure i18n — Arabic and French
- Set up router with role-aware navigation (role comes from a mock auth context for now)
- Set up mock data files

**Dashboard-specific components:**
- AppShell — Sidebar + main area wrapper
- Sidebar — fixed left navigation with BrandHeader, NavLinks, BottomActions
- NavigationItem — icon, label, badge, active state (brand.sageLight background, brand.primary text)
- TopBar — tab navigation left, operator name and avatar right
- PageContent — scrollable content area with surface.background canvas
- DataCard — panel with CardHeader and body slot
- DataTable — sortable, paginated table with row actions

**Mock data (src/mocks/):**
- `partners.ts` — 5 partners
- `stations.ts` — same 15 stations as driver apps
- `chargers.ts` — same chargers
- `users.ts` — 10 mock users with roles
- `reviews.ts` — same reviews
- `reports.ts` — mock stat values for all KPI cards

**Partner mode screens (role = partner):**
- Overview: 4 StatCards (stations, chargers, reviews, availability), DataCard with own station list
- My Stations: DataTable with name, charger count, status, row actions (edit, manage chargers, update availability)
- Station Edit: form with Input, Select for charger management — static
- Charger Management: DataTable with connector type, power, status, row actions
- Availability Update: DataTable with toggle controls — static
- Reports: 4 StatCards, DataCard with usage chart placeholder

**Admin mode screens (role = admin):**
- Overview: 6 StatCards (total users, partners, stations, chargers, reviews, events), DataCard with live station list, DataCard with active drivers table
- Users: DataTable with name, email, role, status, row actions
- Partners: DataTable with name, station count, row actions
- Stations: DataTable with name, partner, status, row actions
- Chargers: DataTable with station, connector type, power, status
- Reviews: DataTable with station, user, rating, text, moderation action buttons
- Reports: 6 StatCards, DataCard with chart placeholders

**Role switching (dev only):**
- A dev-only toggle in the UI to switch between partner and admin mock roles
- This toggle is removed in Phase 4 when real auth is introduced

**RTL:**
- Sidebar switches direction in Arabic
- Tables align correctly in RTL
- Forms align correctly in RTL

**Done when:**
- ✅ All partner screens navigable with mock data
- ✅ All admin screens navigable with mock data
- ✅ Role switching works correctly
- ✅ Arabic RTL correct on all screens
- ✅ No backend calls anywhere

### Sprint 1.5 — Phase 1 Hardening (1 week)

**Goal:** All three apps are solid, consistent, and RTL-correct before any backend work begins.

**Tasks:**

**Cross-app consistency review:**
- Verify StatusBadge renders identically across Driver Web, Driver Mobile, and Dashboard
- Verify StationCard is visually consistent between Driver Web and Driver Mobile
- Verify color tokens resolve to the same hex values in Tailwind and in native styles
- Verify brand.primary (#007943) appears correctly in all active states across all apps

**RTL audit — every screen in every app:**
- Switch to Arabic in Driver Web — verify every screen
- Switch to Arabic in Driver Mobile (iOS and Android) — verify every screen
- Switch to Arabic in Dashboard — verify every screen
- Document any RTL failures as BUG entries in `docs/project/bugs.md`
- Fix all Class A RTL bugs before this sprint closes

**Accessibility audit (Driver Web and Dashboard):**
- Keyboard navigation works on all interactive elements
- Focus indicators visible
- Color contrast meets WCAG 2.1 AA on all text/background combinations
- Status colors (green/amber/red) have non-color indicators (dot + text label)

**Cross-browser test (Driver Web and Dashboard):**
- Chrome — all screens
- Firefox — all screens
- Safari — all screens

**Mobile device test (Driver Mobile):**
- iOS simulator — all screens
- Android emulator — all screens
- Test with large font size setting on both platforms

**Documentation:**
- Update `docs/ui/screens.md` to exactly match what was built
- Update `docs/ui/components.md` to exactly match what was built
- Update `docs/ui/design-tokens.md` to exactly match token values
- Write `docs/guides/onboarding.md` section for running all three apps locally

**Phase 1 Done When:**
- ✅ All three apps run locally with `pnpm dev`
- ✅ All screens navigable in all three apps
- ✅ Zero Class A bugs
- ✅ RTL correct on every screen in all three apps
- ✅ Cross-browser test passed
- ✅ iOS and Android smoke test passed
- ✅ All documentation reflects reality

---

## Phase 2 — Database Foundation

**Goal:** PostgreSQL + PostGIS is running with all four schemas, all tables, OSM data imported, and dev seeds producing realistic data. No services yet — just the database layer.

**Duration:** 8 weeks (4 sprints × 2 weeks each)

[Detailed sprint breakdowns follow the same pattern as Phase 1 in the provided plan]

---

## Phase 3 — Backend Services

**Goal:** All three backend services are implemented, tested against the real database, and running in Docker Compose. Apps still use mock data — connection happens in Phase 5.

**Duration:** 8 weeks (4 sprints × 2 weeks each)

---

## Phase 4 — Authentication and User Management

**Goal:** Keycloak is running, JWTs are issued with correct claims, middleware enforces roles in both services, first-login provisioning works, and auth flows are complete in both driver apps.

**Duration:** 12 weeks (6 sprints × 2 weeks each)

---

## Phase 5 — Connect Apps to Services

**Goal:** Mock data is replaced by real API calls. All three apps talk to real services. Every screen shows real data from the database.

**Duration:** 10 weeks (5 sprints × 2 weeks each)

---

## Phase 6 — GIS Synchronization

**Goal:** PostgreSQL trigger syncs station data to GIS layer. Driver Service uses real spatial queries against GIS layer. Map reflects real station positions.

**Duration:** 5 weeks (3 sprints: 2+2+1 weeks)

---

## Phase 7 — Clickstream Analytics

**Goal:** All three apps fire analytics events. Events persist through Clickstream Service into PostgreSQL. Aggregate jobs run. Admin Dashboard shows real analytics data.

**Duration:** 5 weeks (3 sprints: 2+2+1 weeks)

---

## Phase 8 — Traefik and Production Runtime

**Goal:** Traefik is the single public entrypoint. TLS works. All services are behind it. Production Compose config is validated.

**Duration:** 4 weeks (2 sprints: 2+2 weeks)

---

## Phase 9 — Features, Hardening, and Launch Readiness

**Goal:** Complete the business features deferred during infrastructure phases. Harden the platform for real users.

**Duration:** 10 weeks (5 sprints: 2+2+1+2+2 weeks)

**Key features:**
- Favorites feature (full flow)
- Reviews feature (with moderation)
- Profile management
- Security and performance hardening
- Final launch readiness verification

---

## Delivery Summary

| Metric | Value |
|--------|-------|
| **Total Phases** | 9 |
| **Total Sprints** | 34 sprints |
| **Sprint Duration** | 2 weeks each (except hardening sprints at 1 week) |
| **Estimated Timeline** | ~68 weeks (~16 months) |
| **With Parallelization** | Can be compressed by running certain sprints in parallel |

---

## Key Principles

### Layered Delivery
- **Phase 1:** UI layer (no backend)
- **Phase 2:** Data layer (no services)
- **Phase 3:** Service layer (no connection)
- **Phase 4:** Authentication (no app integration)
- **Phase 5:** Integration (mock replaced with real)
- **Phase 6:** Enrichment (GIS sync)
- **Phase 7:** Analytics (event tracking)
- **Phase 8:** Infrastructure (production runtime)
- **Phase 9:** Features (business logic completion)

### Nothing is Thrown Away
Mock data is replaced, not rebuilt. Component stubs become real implementations. Every layer builds on the previous one.

### Visible and Testable
Every phase ends with a hardening sprint that must pass before the next phase begins. Users see progress at the end of each phase.

### Progressive Complexity
Early phases are low-risk (UI only). Late phases handle high-complexity integration. Risk is managed by building simple things first.

---

## Critical Checkpoints

Each phase closes with a hardening sprint. A phase must achieve:

- ✅ All planned tasks complete (or explicitly deferred with written reason)
- ✅ Zero Class A bugs
- ✅ All tests pass
- ✅ Documentation updated to reflect reality
- ✅ Written phase summary in `docs/project/phases/phase-NN-status.md`

Before advancing to the next phase, all checkpoints must be verified.

---

**Document Version:** 1.0  
**Status:** Active, Ready for Sprint Planning  
**Last Updated:** 2026-06-05
