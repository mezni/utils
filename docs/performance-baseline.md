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
