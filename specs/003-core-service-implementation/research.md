# Research: Phase 2 — Core Service Implementation

**Feature**: [spec.md](./spec.md)
**Plan**: [plan.md](./plan.md)
**Date**: 2026-05-23

## Purpose

This document resolves technical decisions for the Phase 2 core service implementation. All decisions were validated during the specification and clarification phase.

## R-001 — Technology Stack

**Context**: The technical context indicates TypeScript/Node.js but needs clarification.

**Decision**: Use Rust with Actix Web framework.

**Rationale**:
- Rust provides superior performance, memory safety, and concurrency
- Actix Web is one of the fastest web frameworks available
- Rust's strong type system prevents entire classes of bugs at compile time
- Excellent for building high-performance, reliable microservices
- Aligns with the constitution's specification that geo-service is implemented in Rust

**Alternatives considered**:
- **TypeScript/Node.js**: Rejected. While productive, it doesn't provide the same level of performance and type safety as Rust.
- **Go**: Rejected. While also performant, Rust's ownership model provides better memory safety guarantees.

## R-002 — Primary Dependencies

**Context**: The technical context mentions NestJS, PostgreSQL, TypeORM, JWT but needs specific versions and additional dependencies for Rust implementation.

**Decision**: 
- **Framework**: Actix Web 4.x
- **Database**: SQLx 0.7 with PostgreSQL driver
- **Authentication**: jsonwebtoken 8.x, oauth2 4.x
- **Validation**: validator 0.16, serde 1.x
- **Documentation**: paperclip 0.8 or utoipa 3.x
- **Testing**: tokio-test 0.4, reqwest 0.11
- **Events**: lapin 2.x (RabbitMQ client)
- **Configuration**: config 0.13
- **Serialization**: serde 1.x, serde_json 1.x
- **Error Handling**: thiserror 1.x, anyhow 1.x
- **Logging**: tracing 0.1, tracing-subscriber 0.3
- **Async Runtime**: tokio 1.x

**Rationale**:
- Actix Web is one of the fastest and most mature web frameworks in Rust
- SQLx provides compile-time checked SQL queries without sacrificing performance
- The Rust ecosystem provides excellent libraries for all required functionality
- All selected crates are well-maintained and have good community support

**Alternatives considered**:
- **Diesel instead of SQLx**: Rejected. SQLx's async support and compile-time verification is better suited for our use case.
- **Axum instead of Actix Web**: Rejected. While Axum is excellent, Actix Web has more mature middleware and ecosystem for enterprise applications.

## R-003 — Testing Framework

**Context**: The technical context needs testing strategy clarification for Rust implementation.

**Decision**: Use Rust's built-in testing framework with additional crates for comprehensive testing per Principle VII:

1. **Unit tests**: Rust's built-in `#[test]` for domain logic, services, utilities
2. **Integration tests**: testcontainers-rs for database, queue, HTTP
3. **Transaction tests**: SQLx transaction testing with rollback
4. **Outbox tests**: Mock RabbitMQ broker with testcontainers
5. **Audit-log tests**: Repository testing with test database
6. **Soft-delete tests**: Behavior verification with test cases
7. **E2E tests**: reqwest for full API stack testing

**Rationale**:
- Rust's built-in testing framework is excellent and requires no additional dependencies for unit tests
- testcontainers-rs enables realistic integration testing with actual PostgreSQL
- The combination covers all testing requirements from Principle VII
- Rust's testing philosophy aligns well with our quality requirements

**Alternatives considered**:
- **Custom test framework**: Rejected. Rust's built-in testing is sufficient and well-designed.
- **Separate E2E tool**: Rejected. reqwest provides clean HTTP testing and integrates well with Rust's async ecosystem.

## R-004 — Target Platform

**Context**: The technical context mentions Linux server but needs deployment specifics for Rust implementation.

**Decision**: Target Ubuntu 22.04 LTS with Docker containerization using multi-stage builds.

**Rationale**:
- Ubuntu 22.04 LTS is stable, well-supported, and commonly used in production
- Rust's static compilation enables very small, secure Docker images
- Multi-stage builds minimize final image size and attack surface
- Aligns with the constitution's docker-compose deployment model
- Containerization simplifies dependency management and scaling

**Alternatives considered**:
- **Alpine Linux base image**: Rejected. While smaller, musl libc can cause issues with some Rust crates, and Ubuntu has better compatibility.
- **Bare metal deployment**: Rejected. Containerization provides better isolation and aligns with microservices architecture.

## R-005 — Database Connection Strategy

**Context**: The specification mentions PostgreSQL with proper connection handling but needs strategy details for Rust implementation.

**Decision**: Implement connection pooling with SQLx and handle connection failures gracefully.

**Strategy**:
- **Connection Pool**: SQLx connection pool with 10 minimum, 20 maximum connections
- **Connection Failure Handling**: 
  - Automatic retry with exponential backoff using retry policies
  - Circuit breaker pattern for prolonged outages
  - Graceful degradation when database is unavailable
  - Health check endpoint that reflects database status
- **Migration Strategy**: SQLx migrations with transactional rollbacks using sqlx-cli

**Rationale**:
- Connection pooling improves performance under load
- SQLx provides compile-time verified queries and excellent async support
- Graceful failure handling prevents cascading failures
- Circuit breaker prevents hammering a failing database
- Health checks enable proper monitoring and alerting

**Alternatives considered**:
- **Single connection per request**: Rejected. Poor performance under load, doesn't scale well.
- **External connection pool (PgBouncer)**: Rejected. Adds complexity; SQLx's built-in pooling is sufficient for Phase 2.

## R-006 — JWT Validation Strategy

**Context**: The specification mentions JWT validation via Authorization header but needs implementation details for Rust implementation.

**Decision**: Implement dual JWT validation at gateway AND service level with proper error handling.

**Strategy**:
- **Gateway Validation**: NGINX validates JWT signature and basic claims
- **Service Validation**: Core-service re-validates JWT signature and checks user permissions
- **Token Format**: JWS with RS256 algorithm
- **Claims Validation**: 
  - Issuer (iss) validation
  - Audience (aud) validation
  - Expiration (exp) validation
  - User ID and role extraction
- **Error Handling**: Return 401 for invalid tokens, 403 for insufficient permissions
- **Implementation**: Use jsonwebtoken crate with custom validation middleware

**Rationale**:
- Dual validation provides defense-in-depth as required by Principle V
- RS256 provides better security than HS256 for microservices
- Rust's strong type system helps prevent JWT validation errors
- Comprehensive claim validation prevents token tampering
- Clear error responses enable proper client handling

**Alternatives considered**:
- **Gateway validation only**: Rejected. Violates Principle V which requires independent validation at each service.
- **Shared secret (HS256)**: Rejected. Less secure for microservices; RS256 with proper key management is more secure.

## R-007 — API Versioning Implementation

**Context**: The clarification specified URL path versioning (/api/core/v1/) but needs implementation details for Rust implementation.

**Decision**: Implement URL path versioning with Actix Web routing.

**Strategy**:
- **Route Structure**: /api/core/v1/{resource}
- **Version Headers**: Also support Accept: application/vnd.api.v1+json
- **Deprecation Policy**: v1 supported for 12 months after v2 release
- **Documentation**: OpenAPI spec clearly documents v1 endpoints
- **Implementation**: Use Actix Web's nested routing with version-specific modules

**Rationale**:
- URL path versioning is clear and easy to understand
- Actix Web provides flexible routing that supports versioning
- Supporting both URL and header versioning provides flexibility
- Clear deprecation policy enables smooth upgrades
- Rust's module system naturally supports version separation

**Alternatives considered**:
- **Header versioning only**: Rejected. Less intuitive for developers and harder to test in browsers.
- **Query parameter versioning**: Rejected. Not RESTful, can be cached incorrectly, and is less discoverable.

## R-008 — Optimistic Concurrency Control

**Context**: The clarification specified optimistic concurrency with version/timestamp but needs implementation details for Rust implementation.

**Decision**: Implement optimistic concurrency using version numbers with SQLx.

**Strategy**:
- **Version Field**: Add `version: i32` field to all entities that support concurrent updates
- **Automatic Versioning**: Implement custom version incrementing in update operations
- **Update Logic**: 
  - Read entity with current version
  - Update with WHERE id = $1 AND version = $2
  - Check affected rows; if 0, throw optimistic lock exception
- **Error Handling**: Return HTTP 409 (Conflict) with clear error message
- **Retry Strategy**: Client-side retry with exponential backoff suggested in error response
- **Implementation**: Use SQLx's query builder for atomic updates

**Rationale**:
- Optimistic locking performs better than pessimistic locking for read-heavy workloads
- Rust's ownership model helps prevent concurrent access bugs at compile time
- HTTP 409 is the standard status code for concurrency conflicts
- Clear error messages enable proper client handling
- SQLx provides compile-time verification of update queries

**Alternatives considered**:
- **Timestamp-based concurrency**: Rejected. Version numbers are simpler and avoid clock synchronization issues.
- **Pessimistic locking**: Rejected. Performs poorly for read-heavy workloads and can cause deadlocks.

## R-009 — Error Response Format

**Context**: The clarification specified JSON error responses with error code, message, and details but needs format details for Rust implementation.

**Decision**: Implement standardized error response format following RFC 7807 (Problem Details).

**Format**:
```json
{
  "type": "https://api.bornemap.tn/errors/concurrent-modification",
  "title": "Concurrent Modification",
  "status": 409,
  "detail": "The resource was modified by another transaction",
  "instance": "/api/core/v1/companies/CMP-123456789",
  "errors": [
    {
      "field": "version",
      "message": "Current version is 2, but expected 1"
    }
  ]
}
```

**Strategy**:
- **Error Types**: URI that uniquely identifies the error type
- **HTTP Status**: Always match the response status code
- **Validation Errors**: Include field-level errors in the errors array
- **Documentation**: All error types documented in OpenAPI spec
- **Localization**: Support Accept-Language header for localized messages
- **Implementation**: Use serde for serialization and custom error types with thiserror

**Rationale**:
- RFC 7807 is the standard for HTTP API error responses
- Consistent format enables programmatic error handling
- Detailed validation errors improve developer experience
- Type URIs enable machine-readable error classification
- Rust's strong type system helps ensure error response consistency

**Alternatives considered**:
- **Custom error format**: Rejected. Reinventing the wheel; RFC 7807 is well-established.
- **Simple {error: message} format**: Rejected. Insufficient for complex validation errors and programmatic handling.

## R-010 — OpenAPI Documentation Strategy

**Context**: The specification requires a complete OpenAPI specification but needs generation strategy for Rust implementation.

**Decision**: Use utoipa with automatic generation from code attributes.

**Strategy**:
- **Automatic Generation**: Use utoipa attributes on handlers, structs, and enums
- **Documentation Quality**:
  - Detailed descriptions for all endpoints
  - Example requests/responses using serde
  - Authentication requirements clearly documented
  - Error responses documented
- **Versioning**: Separate OpenAPI spec for each API version
- **Accessibility**: Available at /api/core/v1/api-json and /api/core/v1/docs (Swagger UI via utoipa-swagger-ui)
- **Validation**: CI pipeline validates OpenAPI spec completeness

**Rationale**:
- Automatic generation ensures documentation stays in sync with code
- utoipa has excellent Rust support and integrates well with Actix Web
- Rust's attribute system allows for clean, declarative documentation
- Comprehensive documentation reduces integration errors
- CI validation prevents documentation drift

**Alternatives considered**:
- **Manual OpenAPI specification**: Rejected. Prone to drift and requires manual maintenance.
- **paperclip**: Rejected. While good, utoipa has better Actix Web integration and more active development.