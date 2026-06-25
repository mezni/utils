# Sprint 08 — Build auth-service (Identity Integration Foundation)

**Constitution:** BorneMap v1.15.2
**Service:** auth-service
**Port:** 3000

## Sprint Goal

Implement the first version of auth-service as the owner of the users schema and the bridge between Keycloak identity and BorneMap user profiles.

At the end of this sprint:
- auth-service starts successfully.
- Database connectivity is operational.
- JWT validation against Keycloak is operational.
- User profile APIs are available.
- User profile auto-provisioning works.
- Health endpoints work.
- SQLx compile-time validation passes.
- No frontend integration yet.

## Scope

### Included
- Service Foundation (Rust bootstrap, Axum, SQLx, config, logging, health)
- Identity (Keycloak JWT validation, JWKS retrieval, user extraction)
- User Domain (read/create/update profile, auto-provisioning)
- Database (users schema access, user_profiles ownership)

### Excluded
- Social login, MFA, admin management, RBAC enforcement
- Session revocation, password management
- Frontend login flows

## Service Structure

```
services/auth-service/src/
├── api/
│   ├── mod.rs
│   ├── health.rs
│   └── profile.rs
├── domain/
│   ├── mod.rs
│   └── user_profile.rs
├── infrastructure/
│   ├── mod.rs
│   ├── config.rs
│   ├── database.rs
│   └── keycloak.rs
├── repository/
│   ├── mod.rs
│   └── user_profile_repository.rs
├── services/
│   ├── mod.rs
│   └── profile_service.rs
├── lib.rs
├── state.rs
├── router.rs
└── main.rs
```
