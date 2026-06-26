# Sprint 03 — Authentication Logic

**ID:** 03  
**Name:** auth-logic  
**Status:** Planned  
**Date:** 2026-06-26  

## Scope

| Area | Extent |
|---|---|
| `shared/bornemap-core` | User, UserRole, AuthError, UserRepository trait |
| `shared/bornemap-auth` | JWT issuance (sign + verify) |
| `services/auth-service` | Full register/login flow |

## Architecture Mapping

```
HTTP (register, login handlers → DTOs)
  ↓
Application (RegisterUseCase, LoginUseCase)
  ↓
Infrastructure (PgUserRepository, PasswordService, JwtService)
  ↓
bornemap-core / bornemap-auth (domain types, JWT primitives)
  ↓
PostgreSQL
```

## Implementation Order

1. bornemap-core: domain types + errors
2. bornemap-auth: JWT issuance + validation
3. auth-service infrastructure: repo, password, JWT integration
4. auth-service application: use cases
5. auth-service HTTP: handlers, DTOs, errors
6. auth-service config + DI wiring
7. Tests
8. Verify

## Test Strategy

- Unit: password hash/verify, JWT roundtrip, use cases
- Integration: register + login endpoints against real DB
- Edge cases: duplicate email, invalid credentials, malformed input

## Security Checklist

- [x] Argon2id password hashing
- [x] No plaintext passwords stored
- [x] JWT with configurable secret (env)
- [x] Input validation (email format, password min length)
- [x] No unwrap() in production code
