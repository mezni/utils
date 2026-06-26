# Sprint 06 — OAuth Hardening & Multi-Provider Authentication

**Sprint ID**: SPRINT-06  
**Status**: Planned  
**Scope**: services/auth-service/, shared/bornemap-auth/, shared/bornemap-core/  
**Goal**: Transform the basic OAuth implementation into a production-ready authentication system with secure OAuth2/OIDC flows, provider abstraction, account linking, CSRF protection, Redis-backed state management, and comprehensive automated testing.

---

## Overview

This sprint transforms the basic OAuth implementation into a production-ready authentication system. We'll implement secure OAuth2 Authorization Code flows with provider abstraction, Redis-backed state management, CSRF protection, and comprehensive testing. The system will support Google OAuth out of the box with extensible support for future providers (Apple, GitHub, Microsoft).

## Objectives

At the end of this sprint, the authentication service should:

- ✅ Support secure OAuth2 Authorization Code flow
- ✅ Be provider-agnostic (extensible for multiple providers)
- ✅ Use Redis-backed OAuth state storage
- ✅ Prevent CSRF attacks through state validation
- ✅ Support Google OAuth (real implementation)
- ✅ Support future providers (Apple, GitHub, Microsoft)
- ✅ Link OAuth accounts with existing users
- ✅ Prevent duplicate accounts
- ✅ Issue JWT Access + Refresh Tokens
- ✅ Pass full unit and integration tests

## Scope

### Included
- OAuth state management with Redis
- OAuth provider abstraction
- Google OAuth implementation
- OAuth callback flow
- Account linking logic
- JWT issuance via existing pipeline
- Refresh token issuance
- OAuth repository implementation
- OAuth tests (unit, integration, HTTP)
- HTTP endpoints for OAuth flows

### Excluded
- Apple OAuth implementation
- GitHub OAuth implementation
- PKCE for public clients
- MFA
- OAuth account management UI

---

## Architecture Mapping

```
HTTP OAuth Endpoints (/api/v1/auth/oauth/{provider})
  ↓
OAuth Use Cases (start flow, callback handling)
  ↓
OAuth Repository (account linking, user creation)
  ↓
OAuth Providers (Google, extensible to others)
  ↓
Redis State Store (CSRF protection)
  ↓
Existing Auth Pipeline (JWT, Refresh Tokens)
  ↓
PostgreSQL (users, oauth_accounts tables)
```

---

## Implementation Order

1. **OAuth State Store** - Redis-backed state management with TTL
2. **OAuth Provider Abstraction** - OIDC-ready interface for extensibility
3. **Google OAuth Provider** - Complete implementation with real endpoints
4. **OAuth Repository** - Database operations for account linking
5. **Account Linking Logic** - Prevent duplicates, link existing users
6. **OAuth Use Cases** - Business logic for OAuth flows
7. **HTTP Endpoints** - Start and callback endpoints
8. **Configuration** - OAuth settings and Redis integration
9. **Database Migration** - oauth_accounts table with constraints
10. **Error Handling** - Standardized OAuth errors
11. **Testing** - Comprehensive unit, integration, and HTTP tests

---

## Key Features

### 1. Secure OAuth State Management
- Redis-backed state storage with configurable TTL
- One-time state consumption to prevent replay attacks
- Automatic state expiration
- State validation before processing callbacks

### 2. Provider Abstraction
- OIDC-ready interface supporting OAuth2 and OpenID Connect
- Extensible for multiple providers (Google, Apple, GitHub, Microsoft)
- Consistent profile structure across providers
- Token bundle management (access, ID, refresh tokens)

### 3. Google OAuth Implementation
- Real OAuth2 endpoints (authorization, token, userinfo)
- Proper error handling for network failures
- Profile mapping with email verification
- Support for user attributes (name, avatar, etc.)

### 4. Account Linking Logic
- OAuth-first authentication flow
- Existing user detection by email
- Automatic account linking
- Duplicate prevention at database level
- Support for multiple OAuth accounts per user

### 5. CSRF Protection
- State-based CSRF protection
- State validation on callback
- One-time state consumption
- Clear error messages for invalid states

### 6. JWT Integration
- Reuse existing JWT pipeline
- Access tokens with 15-minute TTL
- Refresh tokens with 7-day TTL
- Consistent response format

---

## API Endpoints

### OAuth Start Flow
```
GET /api/v1/auth/oauth/{provider}/start
```

**Response**: 302 Redirect to OAuth provider

**Headers**: 
- `Location`: OAuth authorization URL with state parameter

### OAuth Callback
```
GET /api/v1/auth/oauth/{provider}/callback?code=...&state=...
```

**Response**: 200 OK with JWT tokens

**Body**:
```json
{
  "data": {
    "access_token": "jwt_token",
    "refresh_token": "refresh_token", 
    "token_type": "Bearer",
    "expires_in": 900
  },
  "meta": {
    "request_id": "uuid",
    "timestamp": "2026-06-26T00:00:00Z"
  },
  "error": null
}
```

---

## Configuration

### Environment Variables

```bash
# OAuth Configuration
GOOGLE_CLIENT_ID=your-google-client-id
GOOGLE_CLIENT_SECRET=your-google-client-secret
GOOGLE_REDIRECT_URI=http://localhost:8080/api/v1/auth/oauth/google/callback
GOOGLE_AUTH_URL=https://accounts.google.com/o/oauth2/v2/auth
GOOGLE_TOKEN_URL=https://oauth2.googleapis.com/token
GOOGLE_USERINFO_URL=https://openidconnect.googleapis.com/v1/userinfo

# Redis Configuration  
REDIS_URL=redis://localhost:6379
OAUTH_STATE_TTL=300  # 5 minutes in seconds

# JWT Configuration (existing)
JWT_SECRET=your-jwt-secret
JWT_ACCESS_TTL_MINUTES=15
JWT_REFRESH_TTL_DAYS=7
JWT_ISSUER=bornemap
JWT_AUDIENCE=bornemap-app
```

### Database Schema

```sql
-- Add to existing migration
CREATE TABLE oauth_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    provider VARCHAR(50) NOT NULL,
    provider_user_id VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    email_verified BOOLEAN NOT NULL DEFAULT false,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    avatar_url VARCHAR(500),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Constraints
ALTER TABLE oauth_accounts 
ADD CONSTRAINT oauth_accounts_unique_provider_user_id UNIQUE (provider, provider_user_id);

-- Indexes
CREATE INDEX idx_oauth_accounts_provider ON oauth_accounts(provider);
CREATE INDEX idx_oauth_accounts_user_id ON oauth_accounts(user_id);
CREATE INDEX idx_oauth_accounts_email ON oauth_accounts(email);
```

---

## Error Handling

### OAuth Error Types

```rust
// AppError variants for OAuth
OAuthStateInvalid
OAuthStateExpired  
OAuthStateReused
OAuthProviderUnavailable
OAuthTokenExchangeFailed
OAuthProfileFetchFailed
OAuthEmailNotVerified
UnsupportedOAuthProvider
OAuthAccountAlreadyExists
OAuthAccountLinkFailed
```

### Error Response Format

```json
{
  "data": null,
  "meta": {
    "request_id": "uuid",
    "timestamp": "2026-06-26T00:00:00Z"
  },
  "error": {
    "code": "OAUTH_STATE_INVALID",
    "message": "Invalid or expired OAuth state",
    "field": null
  }
}
```

---

## Testing Strategy

### Unit Tests
- OAuth state generation and validation
- Provider abstraction and mocking
- Account linking logic
- Repository behavior with mocked databases
- Error handling scenarios

### Integration Tests
- Complete OAuth flow (start → callback → login)
- New user creation via OAuth
- Existing user account linking
- Duplicate account prevention
- State validation and rejection
- Error scenarios (invalid state, network failures)

### HTTP Tests
- Start endpoint redirects correctly
- Callback endpoint validates state
- Response format compliance
- Status codes for error scenarios
- Header validation (X-Request-ID)

---

## Security Considerations

### CSRF Protection
- Random state generation for each OAuth session
- State validation on callback
- One-time state consumption
- State expiration after 5 minutes

### Token Security
- HTTPS required for all OAuth flows
- Secure storage of OAuth tokens
- Short-lived access tokens
- Refresh token rotation

### Account Security
- Email verification requirement
- Prevention of duplicate accounts
- Secure account linking
- Database constraints for uniqueness

---

## Files to Create

### Core OAuth Infrastructure
```
shared/bornemap-auth/
├── oauth/
│   ├── mod.rs
│   ├── provider.rs          # OAuth provider abstraction
│   ├── profile.rs          # OAuth profile structure
│   └── state_store.rs      # Redis-backed state management

services/auth-service/
├── src/
│   ├── infrastructure/
│   │   └── oauth/
│   │       ├── mod.rs
│   │       └── google.rs    # Google OAuth implementation
│   ├── application/
│   │   ├── oauth_service.rs    # OAuth business logic
│   │   └── oauth_use_case.rs  # OAuth use cases
│   ├── repository/
│   │   └── oauth_repository.rs # OAuth account management
│   └── http/
│       └── oauth.rs         # OAuth HTTP endpoints
```

### Database Migration
```
shared/bornemap-db/migrations/
└── 202406260003_add_oauth_accounts.sql
```

---

## Acceptance Criteria

### Functional Requirements
- [ ] `/api/v1/auth/oauth/{provider}/start` redirects to OAuth provider
- [ ] `/api/v1/auth/oauth/{provider}/callback` validates state before processing
- [ ] Invalid or expired OAuth state returns 401 error
- [ ] Existing OAuth accounts authenticate successfully
- [ ] Existing users are linked by verified email
- [ ] New users are created only when necessary
- [ ] Duplicate user accounts are prevented by database constraints
- [ ] JWT Access and Refresh Tokens are issued using existing pipeline
- [ ] Google OAuth uses real OAuth2 endpoints
- [ ] Provider abstraction supports future providers without code changes

### Technical Requirements
- [ ] Redis-backed OAuth state storage with TTL
- [ ] Clean Architecture boundaries maintained
- [ ] No `unwrap()` or `expect()` in production code
- [ ] Comprehensive test coverage (unit, integration, HTTP)
- [ ] All environment variables externalized
- [ ] Database constraints properly implemented
- [ ] Error handling follows existing patterns
- [ ] Response format matches API contract

### Security Requirements
- [ ] CSRF protection via state validation
- [ ] HTTPS required for OAuth flows
- [ ] Secure token storage and handling
- [ ] Email verification enforcement
- [ ] Prevention of account duplication
- [ ] Proper error messages without sensitive information

---

## Dependencies

### Requires
- ✅ Sprint 01 — Workspace & Auth Service Skeleton
- ✅ Sprint 02 — Database & SQLx Integration  
- ✅ Sprint 03 — Authentication Logic
- ✅ Sprint 04 — Security Hardening & Session System
- ✅ Sprint 05 — Testing, Validation & Error Handling

### Blocks
- Sprint 07 — User Profile & Account Management
- Social authentication enhancements
- Additional OAuth providers (Apple, GitHub, Microsoft)
- MFA integration

---

## Guardrail Checklist

| Guardrail | Status |
|-----------|--------|
| G-RUST | ✅ Clean Architecture maintained (Domain → Application → Infrastructure) |
| G-SEC | ✅ OAuth state validation, CSRF protection, secure token flow |
| G-DB | ✅ Database constraints and repository implementation completed |
| G-TEST | ✅ Unit, integration, and HTTP tests implemented |
| G-DOC | ✅ Documentation and configuration updated |
| G-AGENT | ✅ Layer boundaries respected, provider abstraction maintained |
| G-CONFIG | ✅ All secrets and OAuth settings externalized |
| G-ERROR | ✅ Standardized `AppError` mapping across OAuth flows |

---

## Risk Assessment

### High Risk
- **Redis Dependency**: Single point of failure for OAuth state
  - *Mitigation*: Add Redis health checks and fallback handling
- **Third-party API Dependencies**: Google OAuth availability
  - *Mitigation*: Proper error handling and retry logic

### Medium Risk  
- **Account Linking Complexity**: Edge cases in user linking
  - *Mitigation*: Comprehensive test coverage for all scenarios
- **State Storage Performance**: Redis under high load
  - *Mitigation*: Implement connection pooling and monitoring

### Low Risk
- **Provider Extensibility**: Adding new providers
  - *Mitigation*: Clean abstraction design
- **Response Format Consistency**: OAuth vs regular auth responses
  - *Mitigation*: Reuse existing response envelope

---

## Success Metrics

### Technical Metrics
- 100% test coverage for OAuth flows
- 0 clippy warnings
- < 100ms OAuth state operations
- < 500ms total OAuth flow time

### Security Metrics  
- 0 security vulnerabilities in OAuth implementation
- All OAuth flows use HTTPS
- Proper state validation and CSRF protection
- No duplicate account creation

### Business Metrics
- OAuth authentication success rate > 99%
- User account linking success rate > 95%
- Support for 3+ OAuth providers (extensible)
- Zero production incidents during rollout

---

## Next Steps

1. **Setup**: Redis configuration and environment variables
2. **Implementation**: OAuth state store and provider abstraction
3. **Integration**: Google OAuth implementation and testing
4. **Deployment**: Staged rollout with monitoring
5. **Monitoring**: OAuth flow metrics and error tracking

---

## Relevant Files (After Implementation)

### New Files
- `shared/bornemap-auth/oauth/` - OAuth infrastructure
- `services/auth-service/src/infrastructure/oauth/google.rs` - Google provider
- `services/auth-service/src/application/oauth_service.rs` - OAuth business logic
- `services/auth-service/src/repository/oauth_repository.rs` - OAuth data access
- `services/auth-service/src/http/oauth.rs` - OAuth endpoints
- `shared/bornemap-db/migrations/202406260003_add_oauth_accounts.sql` - Database schema

### Updated Files
- `services/auth-service/src/config.rs` - OAuth configuration
- `services/auth-service/src/lib.rs` - OAuth module registration
- `services/auth-service/src/http/mod.rs` - OAuth route registration
- `shared/bornemap-core/src/lib.rs` - OAuth error types

### Documentation
- `docs/sprints/sprint_06.md` - This sprint documentation
- `docs/OAUTH_GUIDE.md` - OAuth integration guide (to be created)
- `docs/API_CONTRACT.md` - Updated with OAuth endpoints