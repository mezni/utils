# BorneMap — Phase 2 Refined Execution Plan

## Gantt Timeline

```
Phase 2.1: Infrastructure
  CI/CD Pipeline Setup & Toolchains        Day 1–3     (done in MVP0 Phase 0)
  Docker Compose & Sandbox Seeding         Day 4–7

Phase 2.2: UI & Data Views
  AppShell & Leaflet BaseMap Setup         Day 8–10
  Partner Operator Registry View           Day 11–14
  Station Nearby API Optimization UI       Day 15–19
  Expo Go Mobile Core Verification         Day 20–22
```

## Step-by-Step Execution Path

### Step 1: Establish CI/CD Code Infrastructure Pipelines (3 Days)

> **Note**: CI/CD pipeline setup is now included in MVP0 Phase 0
> (`docs/plan_mvp0.md`). This step is satisfied when the three GitHub
> Actions workflows (`backend.yml`, `frontend.yml`, `docker.yml`) pass
> on the main branch. The remaining items below are supplementary
> tasks that go beyond the MVP0 CI baseline.

- Generate `sqlx-data.json` baseline files to support SQLx offline
  builds as an alternative to the PostgreSQL service container in CI
- Add design token compilation validation tests (verify
  `tailwind.config.ts` produces expected CSS custom properties)
- Configure branch protection rules requiring passing CI checks before
  merge

### Step 2: Launch Local Orchestration Stack & Sandbox Audit (4 Days)

- Deploy the `docker-compose.dev.yml` engine instance locally alongside the `Dockerfile.dev` template
- Initialize schema parameters incorporating soft-delete attributes
- Seed the 5 providers, 100 spatial target stations, and 300 chargers tracking exact Nanoid string definitions

### Step 3: Assemble Core Navigation Shell & BaseMap Views (3 Days)

- Initialize client portal structures under the `sources/frontend/` workspace directory
- Mount the layout container (`<AppShell/>`) and navigation trees
- Mount the map container component leveraging strict CartoDB light background raster inputs
- Ensure floating option widgets layer correctly over maps without clipping viewport zones

### Step 4: Construct the Partner Operator Dashboard View (4 Days)

- Deploy the core partner registry management screen view layout inside the admin portal codebase
- Build out responsive grid components using data toolbar components and text inputs
- Feature segments targeting Business vs Private individual partner fields dynamically
- Force clean string renderings via protection containers (`<ScrollableTable/>`)

### Step 5: Implement and Tune Mobile Nearby Discovery Performance (5 Days)

- Deploy the high-performance `/api/v1/stations/nearby` route in the Rust core backend
- Validate that query calculations execute within the ≤ 200ms latency boundary
- Test against the seeded 100 stations using the default 20km spatial boundary filter capped at 50 returns

### Step 6: Initialize Managed Mobile App Environments via Expo Go (3 Days)

- Scaffold the mobile driver map canvas under `sources/frontend/apps/mobile-driver/`
- Lock explicit third-party libraries without custom compilation parameters
- Execute end-to-end telemetry mapping loops connecting location updates back to versioned Actix-web endpoints
- Lock down performance footprints

## Active Status & Deliverables Milestone Track

| Milestone | Status |
|-----------|--------|
| CI/CD Pipeline Established | ✅ Moved to MVP0 Phase 0 (see `docs/plan_mvp0.md`) |
| Isolation Bug Resolved (`s.is_test` Filtered) | ✅ Verified Mobile Boundary Protection |
| Explicit Tracking & Cascades Fixed | ✅ `partner_profiles` Audit Trail Sealed |
| Seed Primary Keys Format Conformed | ✅ IDs Match 12-Alphanumeric Rule |
| Docker Environment Scaffolding Ready | ✅ `Dockerfile.dev` Created & Aligned |
| Workspace Branch Hierarchy Mapped | ✅ Part 3 Complete Tree Visible |
