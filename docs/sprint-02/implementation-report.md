# Sprint 02 — Implementation Report

## Overview
Sprint 02 implemented the full authentication system in auth-service: registration, login, password hashing (Argon2), JWT issuance, and RBAC scaffolding — all within strict Clean Architecture boundaries.

## What Was Built

### 1. Domain Layer (pure, no framework deps)
- `Account` entity with id, email, password_hash, role, created_at
- `AccountRepository` trait (async) with create/find_by_email/find_by_id
- `DomainError` enum with typed errors (NotFound, AlreadyExists, InvalidCredentials, etc.)

### 2. Application Layer (use cases)
- `AuthUseCases::register()` — validates email/password, hashes password, stores account, returns JWT
- `AuthUseCases::login()` — finds account by email, verifies password, returns JWT
- Input validation: email must contain `@`, password ≥ 8 chars, role must be valid

### 3. Infrastructure Layer
- `PasswordService` — Argon2id hashing with random salt, constant-time verification
- `JwtService` — wraps `common-auth` for HS256 JWT with 24h expiry
- `PostgresAccountRepository` — SQLx implementation with uniqueness constraint violation detection

### 4. Presentation Layer
- `POST /auth/register` — accepts `{email, password, role?}`, returns `{token, email, role}`
- `POST /auth/login` — accepts `{email, password}`, returns `{token, email, role}`
- `GET /health` — returns `{status: "ok", service: "auth-service"}`

### 5. Database
- `0003_create_users_accounts.sql` — `users.accounts` table with UUID PK, unique email, Argon2 hash, role, timestamps

## API Contracts

```
POST /auth/register
→ { "email": "user@example.com", "password": "...", "role": "driver" }
← 201 { "token": "eyJ...", "email": "user@example.com", "role": "driver" }

POST /auth/login
→ { "email": "user@example.com", "password": "..." }
← 200 { "token": "eyJ...", "email": "user@example.com", "role": "driver" }

GET /health
← 200 { "status": "ok", "service": "auth-service" }
```

## Test Results
| Test Group | Count | Status |
|-----------|-------|--------|
| Password hashing (unit) | 4 | ✅ |
| JWT service (unit) | 3 | ✅ |
| Register use case (unit) | 4 | ✅ |
| Integration (API) | 2 | ✅ |
| **Total** | **24** | **✅ All pass** |

## Security Review
| Requirement | Status |
|-------------|--------|
| Argon2 password hashing | ✅ |
| No plaintext passwords stored | ✅ |
| JWT with HS256 + 24h expiry | ✅ |
| Role-based claims in JWT | ✅ |
| Input validation (email, password, role) | ✅ |
| SQL injection prevention (parameterized queries) | ✅ |
| Internal errors never exposed to client | ✅ |
| No unsafe code | ✅ |
