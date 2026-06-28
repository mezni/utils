# Sprint 01 — Technical Plan

## Architecture Impact
- Establishes Rust workspace with shared versioning
- Prevents dependency drift via workspace-level dependency declarations
- Clean Architecture enforced via directory convention (not build-time enforcement yet)

## Module Breakdown

```
bornemap/
├── crates/          # Shared logic (auth, db, errors, types, config)
├── services/        # Microservice implementations
├── apps/            # Frontend applications
├── database/        # SQL migrations
├── docs/            # Documentation
├── scripts/         # Dev tooling
└── .github/         # CI/CD
```

## Dependencies
- Rust 1.90+, Node 20+
- PostgreSQL 15+ with PostGIS
- SQLx compile-time checked migrations

## Risks
| Risk | Mitigation |
|------|------------|
| SQLx migration path misconfiguration | Test migrations via `cargo test` before merging |
| Workspace dependency version conflicts | Single `Cargo.toml` workspace root with pinned versions |
| Frontend build tooling mismatch | Vite 6 + React 19 + Tailwind 4 tested in CI |

## Migration Plan
1. Apply `0001_enable_extensions.sql` → enables UUID, PostGIS, pgcrypto
2. Apply `0002_create_schemas.sql` → creates users, ev, gis schemas
3. No data migrations in Sprint 01
