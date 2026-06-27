# Codebase Review — June 26, 2026

**Review scope:** Full workspace (`shared/` + `services/auth-service/`)  
**Focus:** Architecture, Code Quality, Security, Tests, DevOps  
**Reviewer:** Automated static analysis  

---

## 1. Architecture

### Strengths
- Clean Architecture boundaries maintained: `application/` (use cases), `infrastructure/` (implementations), `http/` (handlers + middleware)
- Domain traits in `bornemap-core` are storage-agnostic
- Use case structs generic over repository traits (`R: UserRepository, S: SessionRepository`)
- Middleware-based observability doesn't leak into business logic
- Redis infrastructure isolated in `bornemap-db` shared crate

### Issues

| # | Finding | Severity | File |
|---|---------|----------|------|
| A1 | **Dual `OAuthStateStore` traits** — `bornemap-auth` defines `create`/`consume`; `auth-service` defines those plus `store_oauth_state`/`validate_oauth_state`. Overlapping and confusing. | Medium | `shared/bornemap-auth/src/oauth/state_store.rs` vs `services/auth-service/src/application/oauth_state.rs` |
| A2 | **`store_oauth_state` and `create` are identical** — both call `set_with_ttl`. Redundant code. | Low | `services/auth-service/src/application/oauth_state.rs:31-38, 44-50` |
| A3 | **`validate_oauth_state` is alias for `consume`** — name implies read-only but it deletes the key. Side effect hidden by naming. | Medium | `services/auth-service/src/application/oauth_state.rs:40-42` |
| A4 | **`redis_config.rs` duplicates `config.rs`** — both parse `REDIS_URL`, `RATE_LIMIT_*`, `OAUTH_STATE_TTL`. DRY violation. | Medium | `services/auth-service/src/redis_config.rs` vs `services/auth-service/src/config.rs` |
| A5 | **`infrastructure/redis.rs` unnecessary nesting** — `mod redis_session;` inside a file when a direct module would be cleaner. | Low | `services/auth-service/src/infrastructure/redis.rs` |
| A6 | **LogoutUseCase unnecessary user lookup** — fetches user only to confirm existence before deleting sessions. The delete itself is a no-op for invalid IDs. | Low | `services/auth-service/src/application/logout.rs:32-41` |

---

## 2. Code Quality

### Strengths
- Error handling uses `Result` + `thiserror` consistently
- `map_err` everywhere (no `unwrap()` / `expect()` in request paths)
- Argon2 password hashing with proper salt
- JWT with `iss`/`aud` validation
- Refresh token rotation with SHA-256 and family revocation
- Redis key namespacing via `RedisKeys` struct
- Atomic `INCR` for rate limiting counters

### Issues

| # | Finding | Severity | File |
|---|---------|----------|------|
| C1 | **Validation errors discarded** — registration handler replaces specific error with generic "Invalid email or password format". User never knows WHY input was rejected. | **High** | `services/auth-service/src/http/auth.rs:45-48` |
| C2 | **RegisterUseCase doesn't validate** — accepts any email/password format, trusts HTTP layer to validate. Bypassable from non-HTTP callers. | **High** | `services/auth-service/src/application/register.rs:27-51` |
| C3 | **Rate limiter TOCTOU race** — `INCR` then `set_with_ttl` is non-atomic. Concurrent requests can both see count=1 and race. Also `set_with_ttl` overwrites value that `INCR` just set. | **High** | `services/auth-service/src/http/middleware/rate_limit.rs:90-96` |
| C4 | **`track_login_attempt` TOCTOU race** — `exists()` → `set_with_ttl()` → `increment()` has a window where two concurrent requests collide. | **High** | `services/auth-service/src/infrastructure/redis_session.rs:110-116` |
| C5 | **RedisClient uses blocking connections** — `get_connection()` returns sync connections. All `async` methods block the tokio thread. Use `get_async_connection()` or enable `aio` feature. | **Critical** | `shared/bornemap-db/src/redis/client.rs` |
| C6 | **Logging uses `info!` for all statuses** — 500 errors logged at `info!` level. Should be `error!` or `warn!` | Low | `services/auth-service/src/http/middleware/logging.rs` |
| C7 | **`oauth_state.clone()` per request** — `GoogleOAuthProvider` contains 3 `String`s cloned every request via `.clone()` in the closure. Should wrap in `Arc`. | Low | `services/auth-service/src/main.rs:64` |
| C8 | **`src/middleware/request_id.rs` is dead code** — `src/middleware.rs` re-exports from new location, but the old file remains. | Low | `services/auth-service/src/middleware/request_id.rs` |

---

## 3. Security

### Strengths
- Argon2 password hashing (memory-hard, resistant to GPU/ASIC cracking)
- JWT with `iss`/`aud`/`exp`/`jti` claims all validated
- Refresh tokens stored as SHA-256 hash (not plaintext)
- Session family revocation on token reuse
- OAuth state with 5-minute TTL, one-time use, replay protection
- Rate limiting middleware (IP-based, configurable)
- Anti-enumeration: login returns same error for wrong password and nonexistent user
- No secrets in source code (all from environment)
- CORS not implemented yet (avoids misconfiguration)

### Issues

| # | Finding | Severity | File |
|---|---------|----------|------|
| S1 | **OAuth provider created with empty secret** — `unwrap_or_default()` on `Option<String>` defaults to `""`. Provider created regardless, fails at runtime with confusing error. | **High** | `services/auth-service/src/main.rs:46-53` |
| S2 | **No password validation in use case** — validation lives only in HTTP handler. Domain-layer callers (tests, cron jobs, future CLIs) bypass it. | **High** | `services/auth-service/src/application/register.rs:27-51` |
| S3 | **No CSRF for non-OAuth endpoints** — only OAuth state has anti-CSRF. Regular session endpoints are unprotected. | Medium | All `POST /api/v1/auth/*` |
| S4 | **No rate limiting on sensitive operations** — only global IP-based rate limit. No specific limits for password reset, email verification, MFA. | Medium | `services/auth-service/src/http/middleware/rate_limit.rs` |
| S5 | **Uuid parse failure returns `InvalidCredentials`** — should return `ValidationError`. | Low | `services/auth-service/src/application/logout.rs:34` |
| S6 | **Login returns 409 for auth failures** — both wrong password and nonexistent user return HTTP 409 (Conflict). Anti-enumeration is good, but 401 (Unauthorized) is more standard. | Info | `services/auth-service/src/http/error.rs` |

---

## 4. Tests

### Strengths
- 8 middleware tests (RequestId, Metrics, Pipeline, Extractor)
- 13 use case tests with mock repositories
- 7 integration tests against real PostgreSQL
- 4 validation unit tests (email/password format)
- bornemap-auth unit tests (JWT roundtrip, expiration, refresh hash)
- Refresh token rotation and family revocation tested
- Email case-insensitivity tested
- Metrics self-exclusion verified

### Issues

| # | Finding | Severity | Details |
|---|---------|----------|---------|
| T1 | **No Redis tests** — OAuth state store, rate limiting, Redis session helpers have ZERO tests. Mock tests were removed in Sprint 07. | **High** | `services/auth-service/src/infrastructure/redis_session.rs`, `src/http/middleware/rate_limit.rs`, `src/application/oauth_state.rs` |
| T2 | **Integration tests leave DB state** — `register_success` creates user `integration-test@example.com` without cleanup. Data persists across runs. | Medium | `services/auth-service/tests/integration.rs` |
| T3 | **No security tests** — no tests for rate limiting (429), OAuth replay, token tampering, SQL injection. | Medium | Entire test suite |
| T4 | **No logout test** — `LogoutUseCase` and `/api/v1/auth/logout` endpoint untested. | Medium | `services/auth-service/src/application/logout.rs` |
| T5 | **No middleware ordering test** — no verification that RequestId runs before Tracing before Logging. | Low | `services/auth-service/tests/middleware_tests.rs` |
| T6 | **Integration tests hardcode Redis URL** — `redis://localhost:6379` in `test_config()` but tests don't need Redis. Connection timeouts if Redis unreachable. | Low | `services/auth-service/tests/integration.rs:18` |

---

## 5. Database & Migrations

### Issues

| # | Finding | Severity | File |
|---|---------|----------|------|
| D1 | **Migration 003 creates already-existing table** — `202406260001_init_auth.sql` already creates `oauth_accounts`. Migration 003 does `CREATE TABLE oauth_accounts` again — will ALWAYS fail. | **Critical** | `shared/bornemap-db/migrations/202406260003_add_oauth_accounts.sql` |
| D2 | **Migration 003 has different schema than 001** — 001 has 5 columns; 003 has 10 columns (adds `email_verified`, `first_name`, `last_name`, `avatar_url`, `updated_at`). If 001 already ran, 003 crashes. Should be `ALTER TABLE`. | **High** | `shared/bornemap-db/migrations/202406260003_add_oauth_accounts.sql` |
| D3 | **Migrations run twice** — mounted as `docker-entrypoint-initdb.d` (PostgreSQL auto-executes) AND via `sqlx::migrate!()` in Rust startup. Risk of duplicate application. | Medium | `infra/docker-compose.yml:14` vs `services/auth-service/src/main.rs:28` |

---

## 6. DevOps / Docker

### Issues

| # | Finding | Severity | Details |
|---|---------|----------|---------|
| O1 | **No auth-service Dockerfile** — `infra/docker-compose.yml:38` references `services/auth-service/Dockerfile` which doesn't exist. | **High** | `infra/docker-compose.yml` |
| O2 | **No TLS termination** — service expects to run behind a reverse proxy. Should be documented. | Low | `services/auth-service/src/main.rs` |

---

## Priority Action Items

### Critical (blocking deployment)
1. Fix migration 003: replace `CREATE TABLE` with `ALTER TABLE` / `CREATE TABLE IF NOT EXISTS`
2. Fix `RedisClient` async: use `get_async_connection()` or enable `aio` feature to avoid blocking tokio

### High (security/stability)
3. Fix rate limiter race: use Lua script `SET NX EX` + `INCR` atomically
4. Fix `track_login_attempt` race: same Lua/atomic approach
5. OAuth provider: guard creation on `client_id AND client_secret`
6. Move password validation into `RegisterUseCase.execute()`
7. Restore specific validation error messages in registration handler
8. Add Redis integration tests

### Medium
9. Consolidate dual `OAuthStateStore` traits
10. DRY up `redis_config.rs` vs `config.rs`
11. Add security tests (rate limit, OAuth replay, token tampering)
12. Add logout endpoint test
13. Fix `info!` vs `error!` level in logging middleware
14. Add CSRF protection for non-OAuth endpoints

### Low
15. Remove dead `src/middleware/request_id.rs`
16. Wrap `GoogleOAuthProvider` in `Arc` instead of cloning strings
17. Fix Uuid parse error to return `ValidationError` instead of `InvalidCredentials`
