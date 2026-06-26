# Sprint 07 — Redis Integration & Security Hardening

**Sprint ID**: SPRINT-07  
**Title**: Redis Integration & Security Hardening  
**Scope**: shared/bornemap-db/, services/auth-service/, shared/bornemap-core/  
**Goal**: Introduce Redis as shared infrastructure for OAuth security, rate limiting, and temporary authentication data while maintaining Clean Architecture and production-grade security.

---

## Overview

This sprint establishes Redis as the shared infrastructure for the BorneMap authentication service. We'll implement Redis as a platform cache and ephemeral data store to support OAuth security, rate limiting, and future authentication features while maintaining strict Clean Architecture boundaries.

## Objectives

At the end of this sprint, the authentication service should:

- ✅ Use Redis as shared infrastructure for OAuth state storage
- ✅ Implement production-ready rate limiting middleware
- ✅ Provide Redis session helpers for future authentication features
- ✅ Maintain Clean Architecture with Redis isolated in Infrastructure layer
- ✅ Support Docker development environment with Redis
- ✅ Pass comprehensive tests for all Redis functionality

---

## Architecture Mapping

```
Domain Layer (Storage Agnostic)
  ↓
Application Layer (Storage Agnostic)
  ↓
Infrastructure Layer (Redis Implementation)
  ↓
Redis (Shared Infrastructure)
```

---

## Implementation Order

1. **Redis Infrastructure** - Shared Redis client and abstraction
2. **OAuth State Storage** - Redis-backed state persistence
3. **Rate Limiting Middleware** - Actix middleware for protected routes
4. **Configuration** - Redis configuration and validation
5. **Session Helpers** - Redis utilities for temporary data
6. **HTTP Integration** - Dependency injection and middleware registration
7. **Docker Setup** - Redis service and environment configuration
8. **Testing** - Comprehensive unit and integration tests

---

## Key Features

### 1. Redis Infrastructure
- **Shared Redis client** with persistent connections
- **Connection management** with proper error handling
- **Async support** for all operations
- **Configuration via environment variables**
- **No Redis APIs outside Infrastructure layer**

### 2. OAuth State Storage
- **Secure anti-CSRF state storage** with 5-minute TTL
- **One-time use** with automatic deletion
- **Replay attack prevention**
- **Proper error handling** for Redis failures

### 3. Rate Limiting Middleware
- **Fixed-window algorithm** with Redis-backed counters
- **Automatic request limiting** for protected endpoints
- **Configurable limits** without code changes
- **HTTP 429 responses** when limits exceeded

### 4. Session Helpers
- **Temporary data storage** with TTL
- **Future authentication features** support (email verification, MFA, etc.)
- **Clean API** for common operations

### 5. Docker Integration
- **Redis service** in development environment
- **Environment configuration**
- **Automatic startup** with local development stack

---

## Configuration

### Environment Variables

```bash
# Redis Configuration
REDIS_URL=redis://redis:6379

# Rate Limiting Configuration
RATE_LIMIT_REQUESTS=100
RATE_LIMIT_WINDOW_SECONDS=60
```

### Configuration Object

```rust
pub struct RedisConfig {
    pub redis_url: String,
    pub rate_limit_requests: u32,
    pub rate_limit_window_seconds: u64,
}

impl RedisConfig {
    pub fn from_env() -> Result<Self, AppError> {
        // Load configuration from environment
    }
}
```

---

## Redis Infrastructure Design

### Module Structure

```
shared/bornemap-db/src/redis/
├── mod.rs           # Module exports
├── client.rs        # Redis client abstraction
├── keys.rs          # Redis key management
└── errors.rs        # Redis error handling
```

### Key Management

```rust
pub struct RedisKeys;
impl RedisKeys {
    pub fn oauth_state(state: &str) -> String {
        format!("oauth_state:{}", state)
    }
    
    pub fn rate_limit(ip: &str) -> String {
        format!("rate_limit:{}", ip)
    }
}
```

---

## OAuth State Storage

### Workflow

```
1. OAuth Start
   ↓
2. Generate secure state
   ↓
3. Store in Redis (TTL: 5min)
   ↓
4. Redirect to provider
   ↓
5. OAuth Callback
   ↓
6. Validate state
   ↓
7. Delete immediately
   ↓
8. Continue authentication
```

### API

```rust
// Application layer API
#[async_trait]
pub trait OAuthStateStore: Send + Sync {
    async fn store_oauth_state(&self, state: &str) -> Result<(), AppError>;
    async fn validate_oauth_state(&self, state: &str) -> Result<(), AppError>;
}

// Implementation in Infrastructure layer
pub struct RedisOAuthStateStore {
    redis_client: Arc<RedisClient>,
}
```

---

## Rate Limiting Middleware

### Protected Routes

- `POST /api/v1/auth/register`
- `POST /api/v1/auth/login`
- `GET /api/v1/auth/oauth/{provider}/start`
- `GET /api/v1/auth/oauth/{provider}/callback`

### Algorithm

**Fixed-Window Counter**
- Redis keys: `rate_limit:<ip>`
- Atomic increment with TTL
- Configurable limits (100 requests/minute/IP by default)

### Middleware Implementation

```rust
pub struct RateLimitMiddleware {
    config: RateLimitConfig,
    redis_client: Arc<RedisClient>,
}

#[derive(Clone)]
pub struct RateLimitConfig {
    pub requests_per_window: u32,
    pub window_seconds: u64,
}

impl<S> actix_web::dev::ServiceFactory<S> for RateLimitMiddleware {
    type Request = ServiceRequest;
    type Response = ServiceResponse;
    type Error = Error;
    type Config = ();
    type Service = RateLimitService<S>;
    type InitError = ();
    type Future = Ready<Result<(Self::Service, Self::Config), Self::InitError>>;
}
```

---

## Redis Session Helpers

### Available Operations

```rust
pub struct RedisSessionHelper {
    redis_client: Arc<RedisClient>,
}

impl RedisSessionHelper {
    pub async fn store_with_ttl(&self, key: &str, value: &str, ttl: Duration) -> Result<(), AppError>;
    pub async fn get(&self, key: &str) -> Result<Option<String>, AppError>;
    pub async fn delete(&self, key: &str) -> Result<(), AppError>;
    pub async fn exists(&self, key: &str) -> Result<bool, AppError>;
}
```

### Future Use Cases

- **Email verification tokens**
- **Password reset links**
- **MFA challenges**
- **Login attempts tracking**
- **Temporary authentication tokens**

---

## HTTP Integration

### Dependency Injection

```rust
// Application state
pub struct AppState {
    pub db: PgPool,
    pub redis_client: Arc<RedisClient>,
    pub rate_limiter: Arc<RateLimitMiddleware>,
}

// Main.rs initialization
let redis_client = RedisClient::new(&config.redis_url).await?;
let rate_limiter = RateLimitMiddleware::new(&config.rate_limit);
```

### Middleware Registration

```rust
HttpServer::new(move || {
    App::new()
        .app_data(web::Data::new(state.clone()))
        .wrap(rate_limiter.clone())
        .configure(http::configure)
})
```

---

## Docker Integration

### docker-compose.yml

```yaml
version: '3.8'
services:
  redis:
    image: redis:8-alpine
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  redis_data:
```

### Environment Configuration

```bash
# .env.example
REDIS_URL=redis://redis:6379
RATE_LIMIT_REQUESTS=100
RATE_LIMIT_WINDOW_SECONDS=60
```

---

## Testing Strategy

### Unit Tests
- Redis client initialization and error handling
- OAuth state storage and validation
- Rate limiting algorithm logic
- Session helper operations

### Integration Tests
- Redis connection management
- OAuth flow with Redis state storage
- Rate limiting middleware behavior
- Docker environment setup

### Test Coverage
- Redis failures mapped to AppError
- Concurrent access scenarios
- TTL expiration handling
- Connection pool management

---

## Security Considerations

### Redis Security
- **Connection security**: TLS support for production
- **Key management**: Structured key names for organization
- **Error handling**: No sensitive data in error messages
- **Access control**: Redis authentication in production

### Rate Limiting Security
- **IP-based limiting**: Prevents brute force attacks
- **Window reset**: Proper TTL management
- **Configurable limits**: Adaptive to different endpoints
- **Error resilience**: Graceful degradation on Redis failures

### Session Security
- **TTL enforcement**: Automatic cleanup of temporary data
- **Key uniqueness**: Collision-resistant key generation
- **Data isolation**: Separate key spaces for different purposes

---

## Files to Create

### Redis Infrastructure
```
shared/bornemap-db/src/redis/
├── mod.rs           # Module exports and types
├── client.rs        # Redis client abstraction
├── keys.rs          # Redis key management
└── errors.rs        # Redis error handling
```

### Application Layer
```
services/auth-service/src/application/
└── oauth_state.rs   # OAuth state store interface

services/auth-service/src/http/middleware/
└── rate_limit.rs    # Rate limiting middleware

services/auth-service/src/infrastructure/
└── redis/           # Redis implementation
```

### Configuration
```
services/auth-service/src/
└── redis_config.rs  # Redis configuration
```

### Docker
```
docker-compose.yml   # Updated with Redis service
.env.example        # Updated with Redis configuration
```

---

## Acceptance Criteria

### Functional Requirements
- [ ] Redis integrated using persistent connection manager
- [ ] Redis isolated within Infrastructure layer only
- [ ] OAuth state stored with 5-minute TTL
- [ ] OAuth state validated and deleted after successful callback
- [ ] Replay attacks prevented through one-time state use
- [ ] Actix rate limiting middleware intercepts protected routes
- [ ] Public endpoints return HTTP 429 when limits exceeded
- [ ] Middleware configurable without code changes
- [ ] Redis injected via dependency injection
- [ ] Docker environment includes Redis service

### Technical Requirements
- [ ] No unwrap() or expect() in runtime request paths
- [ ] All Redis failures mapped to structured AppError values
- [ ] Comprehensive unit and integration tests
- [ ] cargo fmt, cargo clippy, cargo test pass successfully
- [ ] Architecture maintains Clean Architecture boundaries
- [ ] Project guardrails followed (Rust, Security, Testing, etc.)

### Security Requirements
- [ ] Rate limiting prevents brute force attacks
- [ ] OAuth state prevents CSRF and replay attacks
- [ ] Redis failures handled gracefully
- [ ] No sensitive data exposed in error messages
- [ ] Proper TTL management for all temporary data

---

## Dependencies

### Requires
- ✅ Sprint 01 — Workspace & Auth Service Skeleton
- ✅ Sprint 02 — Database & SQLx Integration
- ✅ Sprint 03 — Authentication Logic
- ✅ Sprint 04 — Security Hardening & Session System
- ✅ Sprint 05 — Testing, Validation & Error Handling
- ✅ Sprint 06 — OAuth Hardening & Multi-Provider Authentication

### Blocks
- Sprint 08 — User Profile & Account Management
- Email verification system
- Password reset functionality
- MFA implementation
- Advanced security features

---

## Guardrail Checklist

| Guardrail | Status |
|-----------|--------|
| G-RUST | ✅ Clean Architecture maintained (Infrastructure layer only) |
| G-SEC | ✅ Rate limiting, CSRF protection, proper error handling |
| G-DB | ✅ Redis infrastructure with connection management |
| G-TEST | ✅ Unit, integration, and middleware tests |
| G-DOC | ✅ Documentation and configuration updated |
| G-AGENT | ✅ Layer boundaries respected, dependency injection used |
| G-CONFIG | ✅ All configuration externalized via environment variables |
| G-ERROR | ✅ Redis failures mapped to AppError values |

---

## Risk Assessment

### High Risk
- **Redis Dependency**: Single point of failure for rate limiting and state storage
  - *Mitigation*: Add health checks and graceful degradation
- **Connection Pool Management**: Redis connection exhaustion
  - *Mitigation*: Proper connection pooling and timeout handling

### Medium Risk
- **Rate Limiting Performance**: Redis under high load
  - *Mitigation*: Connection pooling and optimized queries
- **State Storage Consistency**: Race conditions in state validation
  - *Mitigation*: Atomic operations and proper error handling

### Low Risk
- **Configuration Management**: Environment variable complexity
  - *Mitigation*: Clear documentation and validation
- **Docker Integration**: Service availability
  - *Mitigation*: Health checks and auto-restart policies

---

## Success Metrics

### Technical Metrics
- Redis connection establishment time < 100ms
- Rate limiting decision time < 10ms
- State storage/ retrieval time < 5ms
- 100% test coverage for Redis operations

### Security Metrics
- 0 security vulnerabilities in Redis implementation
- Rate limiting effectiveness > 99%
- State storage security audit passed
- No sensitive data exposure in error messages

### Business Metrics
- Authentication system availability > 99.9%
- Rate limiting reduces brute force attacks by 95%
- Redis infrastructure supports future feature expansion
- Development environment setup time < 5 minutes

---

## Next Steps

1. **Setup**: Redis infrastructure and configuration
2. **Implementation**: OAuth state storage with Redis
3. **Security**: Rate limiting middleware implementation
4. **Integration**: HTTP layer dependency injection
5. **Testing**: Comprehensive test coverage
6. **Deployment**: Docker environment setup

---

## Relevant Files (After Implementation)

### New Files
- `shared/bornemap-db/src/redis/` - Redis infrastructure
- `services/auth-service/src/application/oauth_state.rs` - OAuth state interface
- `services/auth-service/src/http/middleware/rate_limit.rs` - Rate limiting middleware
- `services/auth-service/src/infrastructure/redis/` - Redis implementation
- `services/auth-service/src/redis_config.rs` - Redis configuration
- `docker-compose.yml` - Updated with Redis service
- `.env.example` - Updated with Redis configuration

### Updated Files
- `services/auth-service/src/config.rs` - Redis configuration support
- `services/auth-service/src/main.rs` - Redis initialization
- `services/auth-service/src/http/mod.rs` - Middleware registration
- `shared/bornemap-core/src/lib.rs` - Redis error types

### Documentation
- `docs/sprints/sprint_07.md` - This sprint documentation
- `docs/REDIS_INTEGRATION.md` - Redis integration guide (to be created)
- `docs/SECURITY_HARDENING.md` - Security hardening guide (to be created)