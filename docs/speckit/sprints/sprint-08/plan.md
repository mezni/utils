# Sprint 08 Plan — auth-service Identity Integration

## Implementation Order

1. **Cargo.toml** — dependencies (axum, sqlx, tokio, jsonwebtoken, reqwest, serde, chrono, uuid)
2. **Domain** — UserProfile struct with all 9 fields
3. **Infrastructure** — config.rs, database.rs (SQLx pool), keycloak.rs (JWKS fetch + JWT validation)
4. **Repository** — user_profile_repository.rs (CRUD with SQLx, compile-time checked queries)
5. **Services** — profile_service.rs (auto-provisioning on first login)
6. **API** — health.rs (health/ready/live), profile.rs (GET/PUT /me)
7. **State & Router** — AppState, auth middleware, route wiring
8. **Main** — entrypoint with tracing, pool init, keycloak init, server start
9. **Dockerfile** — multi-stage build matching admin-service pattern
10. **Tests** — unit tests for all layers, integration test for API
11. **SQLx prepare** — generate sqlx-data.json for compile-time validation
12. **Delivery** — commit, push, PR

## Key Decisions

- JWT validation uses `jsonwebtoken` crate with JWKS from Keycloak
- Auth extractor with `FromRequestParts` (no middleware, opt-in per route)
- Auto-provisioning: on first authenticated request, insert row into user_profiles
- Matching existing service patterns (admin-service architecture)
