# Performance Baseline — Backend CI

**Date**: 2026-05-25
**Workflow**: `.github/workflows/backend.yml`
**Branch**: `001-project-scaffolding-cicd`
**Runner**: ubuntu-latest (GitHub-hosted)

## Measurements

| Run | Trigger | Duration | Result | Notes |
|-----|---------|----------|--------|-------|
| 1 | Initial push (029e19a) | 25s | ❌ Failed (fmt) | fmt check failed on 8 files |
| 2 | Fix push (2057259) | ~2m 6s | ❌ Failed (clippy) | 7 dead_code errors (all scaffolding stubs) |
| 3 | Fix push (8570d8b) | TBD | 🔄 In progress | Cold cache — Rust crate compilation + sqlx-postgres build |

## Target Baseline

- **Goal**: CI pipeline completes backend checks in under 10 minutes (SC-003 from spec)
- **Cache warm**: Expected ~3-5m (cargo cache from Swatinem/rust-cache)
- **Cache cold**: First run may exceed 10m due to full crate compilation

## Notes

- Timing collected from `gh run list --json startedAt,updatedAt`
- The GitHub Actions cache (`Swatinem/rust-cache@v2`) reduces subsequent runs
- Run 3 includes a 1m cold compile of `sqlx-postgres v0.7.4` (native code)
- After cache is seeded, expected duration: ~3-5 minutes

---

## Nearby Endpoint SLO Benchmark

**Feature**: `003-spatial-discovery-nearby` | **Date**: 2026-05-26
**Endpoint**: `GET /api/v1/stations/nearby`
**Tool**: `scripts/benchmark-nearby.sh` (concurrent curl-based)

### Benchmark Configuration

| Parameter | Value |
|-----------|-------|
| URL | `http://localhost:8080/api/v1/stations/nearby?longitude=10.1815&latitude=36.8065&include_test=true` |
| Requests | 1000 |
| Concurrency | 10 |
| Dataset | 100 seed stations / 300 seed chargers (all `is_test = true`) |
| Database | PostgreSQL 16 + PostGIS 3.4 (Docker, `postgis/postgis:16-3.4-alpine`) |
| Backend | Rust/Actix-web, single binary, `cargo run` dev profile |

### Results

| Run | Avg | Min | P50 | P95 | P99 | Max | SLO (p95 ≤ 200ms) |
|-----|-----|-----|-----|-----|-----|-----|------|
| 1 | 42.34ms | 6.26ms | 17.51ms | 41.33ms | 1777.80ms | 2517.65ms | ✅ PASS |
| 2 | 17.74ms | 6.46ms | 15.73ms | 33.06ms | 47.91ms | 138.22ms | ✅ PASS |

### Analysis

- Both runs comfortably pass the ≤200ms p95 SLO target with significant margin.
- Run 1 shows higher P99/Max due to cold-cache effects (first benchmark hit after container restart). Run 2, executed immediately after, demonstrates stable latency with tight distribution.
- The GIST index on `stations.coordinates` (`idx_stations_coordinates`) provides efficient spatial filtering via `ST_DWithin`.
- The SQL query uses `ST_DWithin` for bounding + `ST_Distance` for ordering, with `COUNT(*) FILTER (WHERE status = 'available')` for available charger count — all in a single round-trip.
- No index or query optimization needed — SLO achieved with existing Phase 1 schema and indexes.
