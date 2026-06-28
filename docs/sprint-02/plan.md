# Sprint 02 — Technical Plan

## Architecture Impact
- auth-service transitions from skeleton to fully implemented
- Clean Architecture enforced with repository pattern
- Domain layer remains framework-agnostic (no actix-web, no sqlx)

## Module Breakdown

### Domain (`domain/`)
```
error.rs         DomainError enum (NotFound, AlreadyExists, InvalidCredentials, InvalidEmail, WeakPassword)
account.rs       Account entity (id, email, password_hash, role, created_at)
repository.rs    AccountRepository trait (create, find_by_email, find_by_id)
```

### Application (`application/`)
```
auth.rs          AuthUseCases struct with register() and login() methods
                 RegisterRequest/Response DTOs
                 LoginRequest/Response DTOs
```

### Infrastructure (`infrastructure/`)
```
password.rs      PasswordService wrapping Argon2 (hash + verify)
jwt_service.rs   JwtService wrapping common-auth (generate + validate)
postgres_repo.rs PostgresAccountRepository implementing AccountRepository trait
```

### Presentation (`presentation/http/`)
```
dto.rs           RegisterRequest, LoginRequest, AuthResponse DTOs
auth.rs          POST /auth/register, POST /auth/login handlers
health.rs        GET /health handler
```

## Dependencies Added
| Crate | Version | Purpose |
|-------|---------|---------|
| argon2 | 0.5 | Password hashing (Argon2id) |
| async-trait | 0.1 | Async trait support for repository |
| thiserror | 2 | Domain error derivation (already in workspace) |

## Risks
| Risk | Mitigation |
|------|------------|
| Argon2 hashing slow in test | Unit tests use direct calls, acceptable CI time |
| JWT secret exposure | Secret loaded from env var, never logged |
| SQL injection via email | SQLx parameterized queries prevent injection |
| Password timing attack | Argon2 constant-time verification (built-in) |

## Migration Plan
1. Apply `0003_create_users_accounts.sql` → creates users.accounts table with unique email index
2. No data migration needed (Sprint 02 is first functional sprint)
