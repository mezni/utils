<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan
at specs/005-integration-testing/plan.md

## Project Structure

source/           ← ALL runtime code
├── services/      ← Rust microservices
│   ├── shared/    ← Shared Rust crates (ev-core, ev-auth, ev-db)
│   ├── driver-service/ ← Rust/Actix :8080
│   └── admin-service/  ← Rust/Actix :8081
├── front/         ← Mobile and web apps
│   ├── packages/   ← Shared design system, UI kit
│   │   ├── tokens/    ← @bornemap/tokens (Phase 3, completed)
│   │   ├── ui/        ← @bornemap/ui (Phase 3, completed)
│   │   └── scripts/   ← Utility scripts (WCAG validation, bundle analysis)
│   ├── mobile-driver/ ← Expo SDK 50 app (Phase 4, complete)
│   ├── web-driver/    ← React + Leaflet (Phase 4, complete)
│   └── dashboard/     ← React + shadcn/ui (Phase 4)
├── pnpm-workspace.yaml ← Workspace configuration
├── package.json ← Root scripts (typecheck, lint, test)
├── tsconfig.base.json ← Shared TypeScript config
└── .eslintrc.cjs ← ESLint configuration
└── .prettierrc ← Prettier configuration

## Phase Status

- **MVP-1 Phase 1**: Complete (backend services, DB)
- **MVP-1 Phase 2**: Complete (backend services validation)
- **MVP-1 Phase 3**: Complete (design system packages)
  - @bornemap/tokens with 9 token categories
  - @bornemap/ui with ThemeProvider + 7 components
  - All WCAG AA contrast checks passing
  - TypeScript strict mode typechecking passing
  - Bundle size < 50KB gzipped
- **MVP-1 Phase 4**: Complete (mobile driver app, web driver app)
  - Mobile (Expo SDK 54, React Native 0.81, Expo Router v6, Zustand, React Query)
  - Web (React 18, Vite, Zustand, React Query, Leaflet)
  - 10 phases completed (181 tasks total)
  - Android production bundle: 3.91 MB (Expo SDK 54, RN 0.81.5, 1379 modules)
  - Services: 16 mobile, 8 web (geolocation, geocoding, stations, offline, notifications, analytics)
  - Stores: theme, station, map (all with persistence)
  - Dark mode with AsyncStorage/localStorage persistence
  - Offline caching (last 50 stations, 5-min TTL)
  - Metro configured with pnpm symlink support (unstable_enableSymlinks, watchFolders, nodeModulesPaths)

## Design System Build Commands

```bash
cd source/front

# Build both packages
pnpm build

# Build tokens only
pnpm build:tokens

# Build UI only
pnpm build:ui

# Typecheck all packages
pnpm typecheck

# Lint all packages
pnpm lint

# Run tests
pnpm test

# Validate WCAG AA contrast
pnpm --filter @bornemap/ui validate-contrast

# Analyze bundle size
pnpm --filter @bornemap/ui analyze-bundle
```

## Design System Artifacts

- Token reference: `design-system/bornemap/MASTER.md`
- Research: `specs/003-design-system-components/research.md`
- Data model: `specs/003-design-system-components/data-model.md`
- Contracts: `specs/003-design-system-components/contracts/package-apis.md`
- Quickstart: `specs/003-design-system-components/quickstart.md`

## Phase 4 Driver Apps Status

- **Feature Branch**: `004-driver-apps` (created)
- **Specification**: Complete with 7 user stories (3 P1, 3 P2, 1 P3), 39 functional requirements, 10 success criteria, 13 edge cases
- **Clarifications**: 2 critical clarifications (observability strategy, OSM rate limit handling)
- **Plan**: Complete (23 days, 10 phases, detailed implementation roadmap)
- **Research**: Complete (10 technical decisions documented)
- **Data Model**: Complete (Station, Charger, StationImage entities defined)
- **API Contracts**: Complete (4 API endpoints + OSM Nominatim contract)
- **Quickstart**: Complete (setup, build, testing instructions)
- **Implementation**: All 10 phases complete (181 tasks done)
- **Build**: Android bundle exported (3.02 MB, ~61s export time)

## Phase 4 Artifacts

### Design Artifacts
- **Plan**: `specs/004-driver-apps/plan.md` - Implementation roadmap, architecture, tech stack
- **Research**: `specs/004-driver-apps/research.md` - 10 technical decisions and best practices
- **Data Model**: `specs/004-driver-apps/data-model.md` - Entity schemas, relationships, validation
- **API Contracts**: `specs/004-driver-apps/contracts/api.md` - REST endpoints and OSM Nominatim
- **Quickstart**: `specs/004-driver-apps/quickstart.md` - Setup, build, testing instructions

### Clarification Resolutions

**Critical Clarifications**:
1. Observability Strategy: JSON structured logging with fetch times and success rates
2. OSM Nominatim Rate Limits: Exponential backoff retry (10s, 30s, 60s) with user-friendly error messages

**Previous Clarifications** (Phase 4 specification):
3. Geocoding API: 10s timeout, 2 retries with linear backoff
4. Image loading: Lazy load when station detail is visible
5. Error recovery: Show error message with copy-to-clipboard button
6. Theme persistence: AsyncStorage (RN) + localStorage (Web)
7. Marker clustering: Cluster badges with counts at 50m radius
8. Refresh frequency: Manual refresh only (no auto-refresh)
9. Offline caching: Cache last 50 stations + station details
10. Web app auth: Public access only (no login)
11. Loading granularity: Per-screen skeletons + minimal global spinner
12. Map provider: react-native-maps (open-source, no API key)
13. pnpm symlinks: Metro configured with unstable_enableSymlinks for pnpm workspace compatibility
14. @babel/runtime: Added as direct dependency for pnpm's non-hoisted transitive dependency resolution

<!-- SPECKIT END -->
