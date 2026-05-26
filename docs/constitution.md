# BorneMap Ecosystem Constitution (v2.0.0)

## 1. Project Overview

BorneMap is a high-performance, multi-tenant geospatial ecosystem designed
for the Tunisian market. The platform focuses on rapid EV charging station
discovery and management, utilizing a modular monorepo pattern to ensure
development speed, system integrity, and strict data isolation.

## 2. Architectural Foundations

*   **Monorepo Workspace:** All services exist within the `sources/` directory.
*   **Backend Core:** Rust compiled binary, Actix-web (async HTTP), SQLx
    (compile-time verified SQL), and Tokio runtime.
*   **Database:** PostgreSQL 16+ with PostGIS. Coordinate system:
    `GEOGRAPHY(Point, 4326)` using longitude-first notation.
*   **Service Level Objective:** Spatial queries (`ST_DWithin`) MUST resolve
    in ≤200ms.

## 3. Security & Data Isolation (Guardrails)

*   **Semantic Identifiers:** No UUIDs. All primary/foreign keys use:
    `[PREFIX]-[12-char-lowercase-alphanumeric-nanoid]`.
    *   `USR-`, `PRT-`, `STN-`, `CHG-`, `CNT-`, `REV-`
*   **Multi-Tenancy:** Partner Dashboard API requests inject the verified
    `owner_id` context into all queries at the extraction tier.
*   **Sandbox Isolation:** Records marked `is_test = true` are strictly
    excluded from production mobile discovery and analytics reporting via
    repository-level filtering
    (`AND ($4 = TRUE OR s.is_test = FALSE)`).
*   **Soft Delete:** `users`, `partner_profiles`, and `stations` carry a
    `deleted_at TIMESTAMPTZ` column. Read queries MUST include
    `WHERE deleted_at IS NULL` unless the caller is an explicit
    admin/audit path.

## 4. Administrative Workspace & UI/UX

*   **Navigation Matrix:** Unified portal partitioned into: Overview, Users,
    Data, Analytics, Security, and Settings.
*   **Design Tokens:** Driven by a centralized Tailwind `tailwind.config.ts`.
    Hardcoded hex codes are banned in view files.
*   **Defensive UI:**
    *   **`<ScrollableTable />`:** Required for any data matrix containing
        relational keys to prevent horizontal layout breakage
        (min-width: 800px).
    *   **Destructive Interlocks:** Destructive actions require manual input
        of the full resource ID (e.g., `STN-4f7d2a8b9c02`) into a
        confirmation modal before the action button unlocks.
*   **Sandbox Indicator:** Admin sessions MUST display a
    `border-t-4 border-sky-500` visual indicator when testing sandbox data.

## 5. Frontend & Mobile Specifications

*   **Mobile Framework:** Managed Expo Go. Ejection
    (`expo eject` / `expo prebuild`) is prohibited. Dependencies MUST be
    locked to exact versions.
*   **Discovery Constraints:**
    *   Nearby search default: 20km radius.
    *   Pagination: Strict hard-cap of 50 records per request.
*   **Map Canvas:** Full viewport canvas layout; all details grids and
    filters MUST layer as top-level overlays.

## 6. Implementation Principles

*   **Modularity:** All domain layers (`/backend/src/domain/`) are built to
    be easily split into standalone microservices.
*   **Deterministic Seeding:** Sandbox environments use a shared seed script
    (`20260525000001_seed_sandbox.up.sql`) that populates 5 partner
    profiles, 100 test stations, and 300 chargers with compliant semantic
    identifiers.
*   **Environment Awareness:** Admin web sessions utilize a persistent
    `border-t-4 border-sky-500` visual indicator when testing sandbox data.

## 7. Versioning

*   **Current Specification:** 2.0.0
*   **Authority:** Master setup finalized 2026-05-25. All new infrastructure
    MUST adhere to these invariants.
