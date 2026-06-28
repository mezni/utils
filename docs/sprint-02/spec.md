# Sprint 02 — Specification

## Goal
Implement the full authentication system in auth-service: user registration, login, password hashing, JWT issuance, and RBAC scaffolding.

## Scope
- auth-service only (no other services modified)
- Clean Architecture with all layers implemented
- users.accounts table with SQLx migration

## Services Affected
- auth-service (full implementation)

## Database Changes
- `0003_create_users_accounts.sql`: accounts table in users schema

## API Endpoints
| Method | Path | Body | Response |
|--------|------|------|----------|
| POST | `/auth/register` | `{email, password, role?}` | `{token, email, role}` |
| POST | `/auth/login` | `{email, password}` | `{token, email, role}` |
| GET | `/health` | — | `{status, service}` |

## Security Requirements
- Argon2 password hashing (never stored in plaintext)
- JWT with HS256, 24h expiry
- Role-based claims in JWT
- Input validation on all endpoints
- Never expose internal errors to client

## Clean Architecture Layers
```
presentation/http/  → DTOs, handlers
application/        → RegisterUseCase, LoginUseCase
domain/             → Account entity, Repository trait, DomainError
infrastructure/     → PostgresAccountRepository, PasswordService, JwtService
```

## Constraints
- No changes to admin-service or driver-service
- No frontend feature implementation beyond auth API scaffold
- Domain layer has no external framework dependencies
