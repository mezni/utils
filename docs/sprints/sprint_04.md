# Sprint 04 — Production Authentication & Session Management

**ID:** 04  
**Name:** production-auth  
**Status:** Completed  
**Date:** 2026-06-26  

## Scope

| Area | Extent |
|---|---|
| `shared/bornemap-core` | Session domain model, SessionRepository trait, AppError expansion |
| `shared/bornemap-auth` | Enhanced JWT service with iss/aud/jti, refresh token utilities |
| `services/auth-service` | JWT refresh token rotation, session persistence, security hardening |

## Architecture Mapping

```
HTTP (login, refresh handlers → DTOs)
  ↓
Application (LoginUseCase, RefreshUseCase)
  ↓
Infrastructure (PgUserRepository, PgSessionRepository, PasswordService, JwtService)
  ↓
bornemap-core / bornemap-auth (domain types, JWT primitives, session management)
  ↓
PostgreSQL (users, sessions tables)
```

## Implementation Order

1. bornemap-core: Session domain model, SessionRepository trait, AppError expansion
2. bornemap-auth: Enhanced JWT service with configurable claims, refresh token utilities
3. auth-service config: AppConfig with JWT TTL, issuer, audience
4. auth-service infrastructure: PgSessionRepository with SQLx
5. auth-service application: Updated LoginUseCase, new RefreshUseCase
6. auth-service HTTP: Updated login handler, new refresh endpoint, error mapping
7. Tests: Unit, integration, security hardening
8. Verify: All tests passing, no unwrap() in production

## Key Features Implemented

### JWT Refresh Token Rotation
- Access tokens: 15-minute TTL (configurable)
- Refresh tokens: 7-day TTL (configurable) 
- Family-based token rotation: all tokens in a family revoked on reuse
- Refresh token hashing with SHA-256 before storage

### Session Management
- Sessions table with family_id, token_hash, expires_at, revoked
- Atomic refresh token rotation: revoke old session → create new session
- Session cleanup: delete expired sessions on startup

### Security Hardening
- No unwrap() in production code
- expect() only in main.rs for startup validation
- Proper error handling with AppError variants
- Argon2id password verification with correct error handling

## Configuration

```bash
# JWT Configuration
JWT_SECRET=your-secret-key-here
JWT_ACCESS_TTL_MINUTES=15
JWT_REFRESH_TTL_DAYS=7
JWT_ISSUER=bornemap
JWT_AUDIENCE=bornemap-app
```

## Test Results

- **Unit Tests**: 23 passing (3 password, 3 JWT, 10 use cases, 7 integration)
- **Security**: All unwrap() removed, proper error handling
- **Integration**: Full auth flow with session management
- **Performance**: Atomic refresh token rotation

## Security Checklist

- [x] Argon2id password hashing with proper error handling
- [x] No plaintext passwords or tokens stored
- [x] JWT with configurable secret, TTL, issuer, audience
- [x] Refresh token rotation with family-based revocation
- [x] Session persistence with proper cleanup
- [x] No unwrap() in production code
- [x] Proper error handling for all authentication scenarios

## Next Steps

- Sprint 05: Driver management and GPS tracking
- Sprint 06: Admin dashboard and reporting
- Sprint 07: Frontend implementation

## Relevant Files

- `shared/bornemap-db/migrations/202406260002_init_sessions.sql`
- `shared/bornemap-core/src/lib.rs` (Session domain, AppError expansion)
- `shared/bornemap-auth/src/lib.rs` (Enhanced JWT service)
- `services/auth-service/src/config.rs` (JWT configuration)
- `services/auth-service/src/application/login.rs` (Updated with session creation)
- `services/auth-service/src/application/refresh.rs` (New refresh use case)
- `services/auth-service/src/infrastructure/pg_session_repo.rs` (Session repository)
- `services/auth-service/src/http/auth.rs` (Updated endpoints)
- `services/auth-service/tests/` (Comprehensive test suite)