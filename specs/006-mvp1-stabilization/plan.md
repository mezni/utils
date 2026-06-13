# Implementation Plan: MVP-1 Stabilization Sprint

**Branch**: `005-integration-testing` | **Date**: 2026-06-13 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `[specs/006-mvp1-stabilization/spec.md](./spec.md)`

## Summary

This stabilization sprint focuses on optimizing MVP-1 performance, polishing UX across mobile apps, and ensuring production readiness before launch. The work includes: reducing interaction latency to <300ms, achieving stable rendering of 1000+ map markers, implementing consistent error recovery, perfecting dark mode across all screens, adding animated skeleton screens, and ensuring 100% reliable event tracking to the analytics database.

Technical approach involves profiling existing applications to identify performance bottlenecks, implementing React Native optimization techniques, tightening PostGIS query optimization, and adding comprehensive testing across iOS 12/13/14+ and Android 10+ devices.

## Technical Context

**Language/Version**: Rust 1.75+ (backend services), TypeScript 5.4 (frontend), React Native 0.81.5 (mobile app)

**Primary Dependencies**: Rust + Actix-web 0.7 (driver/admin services), Expo SDK 54 (mobile app), React 18 (web app), PostGIS 3.4, react-native-reanimated v3, expo-haptics

**Storage**: PostgreSQL 16 + PostGIS (platform_db and analytics_db), AsyncStorage (mobile local cache)

**Testing**: cargo test (backend unit/integration tests), jest (frontend), expo-e2e-tests (mobile end-to-end), performance profiling tools (React DevTools, Chrome DevTools, Xcode Instruments, Android Profiler)

**Target Platform**: Mobile-first - iOS 12+ (iPhone 12/13/14+ recommended), Android 10+ (simulators and physical devices), Web browser (fallback/testing)

**Project Type**: Mobile application with REST API backend (Expo React Native + Rust microservices)

**Performance Goals**: <300ms p95 response time for all user interactions, <5% battery drain per hour during typical usage, 60 fps stable rendering with 1000+ markers

**Constraints**: Mobile app binary size <100MB, zero console errors/warnings, WCAG AA contrast compliance, no marker jitter or unnecessary re-renders, use react-native-reanimated v3 only for animations

**Scale/Scope**: Single driver-facing app, existing infrastructure (2 Rust services, 1 PostgreSQL instance with PostGIS, Traefik gateway), 5 user stories covering performance, stability, UX polish, and observability

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### I. UX-First ✅
- Skeleton screens required over spinners (covers FR-005)
- Optimistic UI on user actions (covered in error recovery UX)
- Haptic feedback on CTAs (applicable to station actions)
- Empty states fully designed (applicable to no-stations-found scenarios)
- Dark mode on every screen (FR-004, SC-005)
- No marker jitter (FR-002, SC-002)
- Animations using react-native-reanimated v3 (covered in optimization phase)

### II. Domain-Driven Services ✅
- Driver service handles discovery/search (already in place)
- Admin service handles event ingestion (already in place)
- No new services or cross-domain queries

### III. Test-First (NON-NEGOTIABLE) ✅
- 80%+ unit test coverage required for backend performance fixes
- 100% contract test coverage for API optimizations
- Integration tests for geospatial query improvements
- E2E tests for all critical user flows

### IV. Source-Rooted Codebase ✅
- All runtime code in `source/` directory
- No runtime code in `docs/`, `infra/`, or root

### V. Immutable Data & Append-Only Analytics ✅
- Analytics database remains append-only (FR-006, SC-007)
- Station/partner soft-deletion preserved (not affected by stabilization)

**GATE STATUS**: ✅ PASSED - All constitution principles aligned with stabilization scope

## Project Structure

### Documentation (this feature)

```text
specs/006-mvp1-stabilization/
├── plan.md              # This file
├── research.md          # Phase 0 output - technical research and best practices
├── data-model.md        # Phase 1 output - data model for performance metrics
├── quickstart.md        # Phase 1 output - stabilization testing workflow
├── contracts/           # Phase 1 output - performance optimization contracts
├── checklists/
│   └── requirements.md  # Specification quality checklist
└── tasks.md             # Phase 2 output - task breakdown (/speckit.tasks)
```

### Source Code (repository root)

```text
source/
├── services/
│   ├── shared/         ← Shared Rust crates (ev-core, ev-db, ev-auth)
│   │   ├── ev-core/     ← Performance metrics, error handling, station/charger/partner models
│   │   ├── ev-db/       ← PostGIS queries, connection pooling, performance utilities
│   │   └── ev-auth/     ← JWT validation stub (MVP-3 scope)
│   ├── driver-service/ ← Performance-optimized station discovery endpoints
│   │   ├── src/
│   │   │   ├── routes/stations.rs      ← Optimized radius queries, payload reduction
│   │   │   ├── queries/                ← N+1 query optimization
│   │   │   └── middleware/             ← Request timeout, logging
│   │   └── tests/
│   └── admin-service/  ← Event tracking improvements
│       ├── src/
│       │   ├── routes/events.rs        ← Event batching, reliability
│       │   └── queries/                ← Analytics DB query optimization
│       └── tests/
└── front/
    ├── packages/       ← Design system packages
    │   ├── tokens/     ← Design tokens (colors, spacing, typography)
    │   └── ui/         ← React Native UI components with skeleton states
    ├── mobile-driver/ ← Performance optimization, dark mode, haptics
    │   ├── app/
    │   │   ├── index.tsx                ← Map optimization, marker clustering
    │   │   ├── stations.tsx              ← Pagination, skeleton screens
    │   │   └── station/[id].tsx          ← Lazy loading, error recovery
    │   ├── components/
    │   │   ├── skeleton/                ← Animated skeleton placeholders
    │   │   ├── error/                    ← Error states with recovery
    │   │   └── dark-mode/                ← Theme provider, contrast checks
    │   ├── store/                        ← Zustand state management
    │   └── services/
    │       ├── geolocation/              ← Location permissions, fallbacks
    │       └── queryClient/              ← React Query caching strategy
    └── web-driver/     ← Performance optimization, memory leak fixes
        ├── src/
        │   ├── pages/
        │   │   ├── stations.tsx           ← Pagination, error recovery
        │   │   └── station/[id].tsx        ← Lazy loading
        │   ├── components/
        │   └── services/
```

**Structure Decision**: This is a stabilization sprint on existing codebase, so no new directory structure. All work focuses on performance optimization, UX polish, and testing in existing paths. Key changes:
- Driver service: Query optimization, payload reduction, request timeout
- Admin service: Event tracking reliability, batching
- Mobile app: React Native performance, dark mode, haptics, skeleton screens
- Web app: Memory leak fixes, performance optimization
- Testing: E2E tests, performance profiling, device testing

## Complexity Tracking

> **No constitution violations** - This stabilization sprint operates within existing architecture and design principles.

## Phase 0: Research & Technical Decisions

Generate `research.md` with findings on:

1. **React Native Performance Optimization**
   - Best practices for 60fps rendering with 1000+ markers
   - Marker clustering strategies (react-native-maps clustering, custom clustering)
   - Map container abstraction patterns to avoid jitter
   - State management optimization (React Query caching, dependency minimization)

2. **PostGIS Query Optimization**
   - Radius search performance patterns
   - GIST index effectiveness for geographic queries
   - Query optimization techniques for PostgreSQL 16
   - Pagination strategies for large result sets

3. **Expo Performance Best Practices**
   - Haptic feedback implementation (expo-haptics API)
   - Dark mode implementation patterns
   - Skeleton screen animation techniques (react-native-reanimated v3)
   - App bundle size optimization strategies

4. **Mobile Performance Profiling Tools**
   - iOS Instruments: Time Profiler, Allocations, Energy Impact
   - Android Profiler: CPU, Memory, Network
   - React Native performance monitoring
   - Automated performance regression testing

5. **Analytics Database Reliability**
   - Event ingestion patterns for append-only tables
   - Batch processing strategies for high-volume events
   - Error handling for database failures
   - Retention policies and monitoring

6. **Web Performance Optimization**
   - Memory leak detection and fixes (Chrome DevTools)
   - React re-render optimization
   - Lazy loading patterns for React Router
   - Bundle size reduction

7. **Cross-Device Testing**
   - iOS device testing matrix (iPhone 12/13/14+)
   - Android version compatibility (10+)
   - Battery testing methodologies
   - Accessibility testing tools (WCAG AA verification)

8. **Error Recovery UX Patterns**
   - Network error handling strategies
   - Retry logic patterns
   - User-friendly error messages
   - Fallback UI states

**Output**: `research.md` with technical decisions, best practices, and implementation approaches

## Phase 1: Design & Contracts

### 1.1 Data Model (data-model.md)

Extract entities from stabilization requirements:

**Performance Metrics Entity**
- `metric_type` (string: response_time, frame_rate, memory_usage, cpu_usage, battery_drain)
- `value_ms` (float: metric value in milliseconds)
- `value_percent` (float: percentage values like battery drain)
- `user_action` (string: which user action triggered metric)
- `device_info` (JSON: device model, OS version, screen size)
- `timestamp` (ISO 8601 UTC)
- `environment` (string: production/staging)

**Error Log Entity** (analytics_db)
- `event_type` (string: network_error, server_error, timeout, permission_denial)
- `user_action` (string: failed action)
- `error_message` (string: error description)
- `device_info` (JSON: device model, OS version)
- `timestamp` (ISO 8601 UTC)
- `recovery_action` (string: button pressed, retry initiated)
- `recovery_success` (boolean: did recovery succeed)

### 1.2 API Contracts (contracts/)

**Optimized Stations List Contract**
- Endpoint: `GET /api/v1/stations?page={page}&per_page={per_page}`
- Response size reduction: Strip null fields from JSON response
- Performance: <200ms p95 response time
- Caching: Support ETag for conditional requests

**Optimized Nearby Stations Contract**
- Endpoint: `GET /api/v1/stations/nearby?lat={lat}&lng={lng}&radius={radius}`
- Performance: <100ms p95 response time
- Payload: Only include essential fields (id, name, address, distance_km, status)

**Event Tracking Contract**
- Endpoint: `POST /api/v1/events` (single event), `POST /api/v1/events/batch` (up to 100 events)
- Reliability: Automatic retry on transient failures, drop on permanent failures
- Batch timeout: 500ms, batch size: 100 max

### 1.3 Quickstart (quickstart.md)

**Stabilization Testing Workflow**

```bash
# 1. Run performance tests
cd source/front/mobile-driver
pnpm performance-test

# 2. Test on physical devices
cd source/front/mobile-driver
pnpm android:profile
pnpm ios:profile

# 3. Check battery impact
pnpm battery-test

# 4. Run accessibility audit
pnpm accessibility-test

# 5. Verify API performance
curl -w "@curl-format.txt" http://localhost:8080/api/v1/stations/nearby?lat=36.8&lng=10.1&radius=50

# 6. Test error recovery
# Simulate network error, verify recovery actions

# 7. Verify dark mode on all screens
# Toggle dark mode, verify contrast ratios on each screen

# 8. Check console for errors
# Run app, verify zero errors/warnings
```

### 1.4 Agent Context Update

Update AGENTS.md between SPECKIT markers to reference this plan.

**Output**: `data-model.md`, `contracts/`, `quickstart.md`, updated `AGENTS.md`

## Phase 2: Task Breakdown (Phase 2 output - NOT created by /speckit.plan)

See `/speckit.tasks` command for detailed task breakdown.

## Success Criteria Validation

All success criteria mapped to tasks:
- **SC-001** (<300ms response): Backend query optimization, frontend caching
- **SC-002** (1000+ markers no jitter): Map optimization, marker clustering
- **SC-003** (zero console errors): Code quality, error handling
- **SC-004** (<5% battery): Performance profiling, optimization
- **SC-005** (WCAG AA dark mode): Theme audit, contrast fixes
- **SC-006** (<100MB app size): Bundle optimization, code splitting
- **SC-007** (100% event tracking): Event tracking reliability, batching
- **SC-008** (iOS 12/13/14+ and Android 10+): Device testing matrix
