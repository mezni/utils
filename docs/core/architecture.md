# ARCHITECTURE SPECIFICATION

**Last Updated**: 2026-06-23  
**Status**: Active  
**Authority**: Core Architecture Document

---

## 1. Overview

The BorneMap EV Dashboard Platform uses **Clean Architecture** to maximize flexibility, testability, and maintainability. This document defines the architectural pattern, layer responsibilities, dependency rules, and technology constraints.

### Core Principle

```
Presentation → Application → Domain → Infrastructure
     ↑         ↑              ↑           ↑
     |         |              |           |
   HTTP       Use-cases    Business     Database
   UI         Logic         Rules        IO
```

Dependencies flow **inward only**. No layer can depend on an outer layer. This ensures the domain (core business logic) is completely independent of frameworks, databases, and UI.

---

## 2. Layer Responsibilities

### 2.1 Domain Layer (Innermost)

**Responsibility**: Core business logic and rules

**Contains**:
- Entity definitions (Partner, Station, Charger)
- Value objects (Status, Power Rating, etc.)
- Repository interfaces (trait definitions)
- Business rule validations
- Domain errors

**Constraints**:
- MUST NOT import from other layers
- MUST NOT depend on frameworks (no Actix, no SQLx)
- MUST NOT perform IO (database, network, file)
- MUST be 100% testable without setup
- MUST use Rust's type system to encode business rules

**Dependencies**: Standard library only

**Example**:
```rust
// Domain: Pure business rules
pub struct Partner {
    pub id: PartnerId,
    pub name: String,
    pub status: Status,
}

impl Partner {
    pub fn deactivate(&mut self) -> Result<(), DomainError> {
        match self.status {
            Status::Active => {
                self.status = Status::Inactive;
                Ok(())
            }
            Status::Inactive => Err(DomainError::AlreadyInactive),
            _ => Err(DomainError::CannotDeactivate),
        }
    }
}
```

### 2.2 Application Layer

**Responsibility**: Use-case orchestration and business workflows

**Contains**:
- Use-case handlers (create, read, update, delete)
- Application services that compose domain logic
- Data transfer objects (DTOs)
- Application errors
- Repository implementation selection

**Constraints**:
- MUST call domain entities and apply business rules
- MUST use repository interfaces (defined in domain)
- MUST NOT directly access databases (delegate to infrastructure)
- MUST NOT handle HTTP concerns (status codes, headers)
- MUST be testable with mocked repositories

**Dependencies**: Domain, standard library

**Example**:
```rust
// Application: Use-case orchestration
pub struct CreatePartnerUseCase {
    repository: Arc<dyn PartnerRepository>,
}

impl CreatePartnerUseCase {
    pub async fn execute(&self, req: CreatePartnerRequest) 
        -> Result<PartnerResponse, ApplicationError> {
        // Validate input
        let partner = Partner::new(req.name, req.email)?;
        
        // Delegate to repository
        let created = self.repository.create(partner).await?;
        
        // Return DTO
        Ok(PartnerResponse::from(created))
    }
}
```

### 2.3 Infrastructure Layer

**Responsibility**: External integration and IO operations

**Contains**:
- Repository implementations (SQLx, database operations)
- Database connection pool management
- External API clients
- File system operations
- Configuration loading
- Logging and observability setup

**Constraints**:
- MUST implement repository interfaces defined in domain
- MUST handle all database-specific logic (SQL, migrations)
- MUST implement resilience (connection pooling, retries)
- MUST NOT contain business logic
- MUST be replaceable (easy to swap implementations)

**Dependencies**: Domain, Application, external libraries (SQLx, Tokio, etc.)

**Example**:
```rust
// Infrastructure: Database integration
pub struct PostgresPartnerRepository {
    pool: PgPool,
}

#[async_trait]
impl PartnerRepository for PostgresPartnerRepository {
    async fn create(&self, partner: Partner) -> Result<Partner, RepositoryError> {
        let id = partner.id.as_str();
        sqlx::query_as::<_, Partner>(
            "INSERT INTO ev.partners (id, name, email, status) 
             VALUES ($1, $2, $3, $4) RETURNING *"
        )
        .bind(id)
        .bind(&partner.name)
        .bind(&partner.email)
        .bind(partner.status.as_str())
        .fetch_one(&self.pool)
        .await
        .map_err(RepositoryError::from)
    }
}
```

### 2.4 Presentation Layer (Outermost)

**Responsibility**: HTTP handling and UI communication

**Contains**:
- HTTP request handlers (Actix-Web)
- Request validation and parsing
- Response serialization
- Status code mapping
- Error to HTTP response conversion
- Middleware setup

**Constraints**:
- MUST NOT contain business logic
- MUST delegate to application services
- MUST NOT access database directly
- MUST handle HTTP concerns only
- MUST serialize to standard response format

**Dependencies**: All inner layers

**Example**:
```rust
// Presentation: HTTP handling
#[post("/api/v1/partners")]
pub async fn create_partner(
    req: web::Json<CreatePartnerRequest>,
    service: web::Data<CreatePartnerUseCase>,
) -> impl Responder {
    match service.execute(req.into_inner()).await {
        Ok(partner) => HttpResponse::Created().json(ApiResponse {
            success: true,
            data: Some(partner),
            error: None,
        }),
        Err(e) => HttpResponse::BadRequest().json(ApiResponse {
            success: false,
            data: None,
            error: Some(e.to_api_error()),
        }),
    }
}
```

---

## 3. Dependency Rules

### 3.1 The Dependency Graph

```
Presentation
    ↓
Application
    ↓
Domain
    ↓
(Nothing - Domain is independent)

Infrastructure (depends on Domain only for trait implementations)
```

### 3.2 Forbidden Dependencies

❌ **Domain CANNOT depend on**:
- Application
- Infrastructure
- Presentation
- Any framework (Actix, SQLx, etc.)
- Any external library except std

❌ **Application CANNOT depend on**:
- Presentation
- Infrastructure directly (only through trait interfaces)
- Framework-specific code

❌ **Infrastructure CANNOT depend on**:
- Presentation
- Application
- Domain implementations (only interfaces)

❌ **Presentation CANNOT depend on**:
- (No restrictions - it's the outermost layer)

### 3.3 Interface Segregation

Use trait interfaces to decouple layers:

```rust
// Domain layer: interface only
pub trait PartnerRepository: Send + Sync {
    async fn create(&self, partner: Partner) -> Result<Partner, RepositoryError>;
    async fn read(&self, id: &PartnerId) -> Result<Option<Partner>, RepositoryError>;
    async fn update(&self, partner: Partner) -> Result<Partner, RepositoryError>;
    async fn delete(&self, id: &PartnerId) -> Result<(), RepositoryError>;
    async fn list_active(&self) -> Result<Vec<Partner>, RepositoryError>;
}

// Infrastructure layer: implementation only
pub struct PostgresPartnerRepository { /* ... */ }

#[async_trait]
impl PartnerRepository for PostgresPartnerRepository { /* ... */ }
```

---

## 4. Data Flow

### 4.1 Request Flow (Presentation → Domain)

```
1. HTTP Request arrives at Presentation layer
2. Handler parses request into DTO
3. Handler calls Application service
4. Service applies business rules from Domain
5. Service calls Repository interface
6. Repository implementation (Infrastructure) executes database query
7. Result flows back up: Infrastructure → Application → Presentation
8. Response serialized to JSON and sent to client
```

### 4.2 Response Flow (Domain → Presentation)

```
Infrastructure Result (database row)
    ↓ (mapped to)
Domain Entity (business object)
    ↓ (converted to)
Application DTO (serializable)
    ↓ (wrapped in)
Presentation ApiResponse (JSON)
```

---

## 5. Testing Strategy by Layer

### 5.1 Domain Layer Testing

- **Type**: Unit tests
- **Mocking**: None needed (pure functions)
- **Speed**: Very fast (no IO)
- **Example**:

```rust
#[test]
fn test_partner_deactivate() {
    let mut partner = Partner::new("acme".to_string(), "contact@acme.com").unwrap();
    assert_eq!(partner.status, Status::Active);
    
    partner.deactivate().unwrap();
    assert_eq!(partner.status, Status::Inactive);
}
```

### 5.2 Application Layer Testing

- **Type**: Unit tests with mocks
- **Mocking**: Repository interface
- **Speed**: Fast (no real IO)
- **Example**:

```rust
#[tokio::test]
async fn test_create_partner_use_case() {
    let mock_repo = MockPartnerRepository::new();
    let service = CreatePartnerUseCase { repository: Arc::new(mock_repo) };
    
    let result = service.execute(CreatePartnerRequest { /* ... */ }).await;
    assert!(result.is_ok());
}
```

### 5.3 Infrastructure Layer Testing

- **Type**: Integration tests
- **Mocking**: None (real database in test env)
- **Speed**: Slower (IO operations)
- **Example**:

```rust
#[tokio::test]
async fn test_postgres_partner_create() {
    let pool = setup_test_db().await;
    let repo = PostgresPartnerRepository { pool };
    
    let partner = Partner::new("acme", "contact@acme.com").unwrap();
    let created = repo.create(partner).await.unwrap();
    assert!(!created.id.is_empty());
}
```

### 5.4 Presentation Layer Testing

- **Type**: Integration/E2E tests
- **Mocking**: Services (application layer)
- **Speed**: Slower (HTTP simulation)
- **Example**:

```rust
#[tokio::test]
async fn test_create_partner_endpoint() {
    let app = create_test_app();
    let resp = app.post("/api/v1/partners")
        .json(&CreatePartnerRequest { /* ... */ })
        .send()
        .await;
    
    assert_eq!(resp.status(), 201);
}
```

---

## 6. Allowed Technologies

### 6.1 By Layer

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Presentation** | Actix-Web | HTTP server |
| **Presentation** | Serde | JSON serialization |
| **Application** | Tokio | Async runtime |
| **Infrastructure** | SQLx | Database access |
| **Infrastructure** | Tokio | Async runtime |
| **Infrastructure** | Tracing | Observability |
| **Domain** | (None allowed) | Keep it pure |

### 6.2 Forbidden Technologies

❌ In **Domain**: Actix, SQLx, Serde (unless in DTOs)
❌ In **Application**: Direct database access
❌ In **Presentation**: Business logic

---

## 7. Module Structure

### 7.1 Rust Backend

```
services/admin-service/
├── src/
│   ├── domain/                    # Pure business logic
│   │   ├── entities/
│   │   │   ├── partner.rs
│   │   │   ├── station.rs
│   │   │   └── charger.rs
│   │   ├── repositories/          # Trait interfaces
│   │   │   ├── partner_repository.rs
│   │   │   ├── station_repository.rs
│   │   │   └── charger_repository.rs
│   │   └── errors.rs              # Domain errors
│   │
│   ├── application/               # Use-case logic
│   │   ├── services/
│   │   │   ├── create_partner.rs
│   │   │   ├── update_partner.rs
│   │   │   └── ...
│   │   ├── dto.rs                 # Data transfer objects
│   │   └── errors.rs              # Application errors
│   │
│   ├── infrastructure/            # External integration
│   │   ├── database/
│   │   │   ├── postgres/
│   │   │   │   ├── connection.rs
│   │   │   │   ├── migrations/
│   │   │   │   └── partner_repository_impl.rs
│   │   │   └── migrations.rs
│   │   ├── config.rs
│   │   └── errors.rs              # Infrastructure errors
│   │
│   ├── presentation/              # HTTP handling
│   │   ├── handlers/
│   │   │   ├── partner_handler.rs
│   │   │   ├── station_handler.rs
│   │   │   └── charger_handler.rs
│   │   ├── middleware/
│   │   ├── response.rs            # Standard response format
│   │   └── error_handler.rs       # HTTP error mapping
│   │
│   └── main.rs                    # App setup
```

### 7.2 React Frontend

```
apps/admin-dashboard/
├── src/
│   ├── domains/                   # Feature domains
│   │   ├── partners/
│   │   │   ├── components/
│   │   │   ├── hooks/
│   │   │   ├── services/          # API clients
│   │   │   └── types.ts
│   │   ├── stations/
│   │   └── chargers/
│   │
│   ├── shared/                    # Shared utilities
│   │   ├── components/
│   │   ├── hooks/
│   │   ├── services/              # HTTP client
│   │   └── types.ts
│   │
│   └── App.tsx
```

---

## 8. Error Handling Strategy

### 8.1 Error Types by Layer

```
Domain Layer          Application Layer         Presentation Layer
├─ DomainError        ├─ ApplicationError       ├─ ApiError
│  ├─ InvalidInput    │  ├─ DomainError        │  ├─ 400 Bad Request
│  ├─ BusinessRule    │  ├─ RepositoryError    │  ├─ 404 Not Found
│  └─ Validation      │  └─ ValidationError    │  └─ 500 Server Error
│                     │
└─ Maps to HTTP via:  └─ Maps to HTTP via:
   ApplicationError      HTTP Status Code
```

### 8.2 Error Flow

```rust
// Domain defines error types
pub enum DomainError {
    InvalidInput(String),
    BusinessRuleViolation(String),
}

// Application wraps domain errors
pub enum ApplicationError {
    Domain(DomainError),
    Repository(RepositoryError),
}

// Presentation converts to HTTP status
impl From<ApplicationError> for HttpResponse {
    fn from(err: ApplicationError) -> Self {
        match err {
            ApplicationError::Domain(DomainError::InvalidInput(_)) 
                => HttpResponse::BadRequest(),
            ApplicationError::Repository(_) 
                => HttpResponse::InternalServerError(),
        }
    }
}
```

---

## 9. Configuration Management

### 9.1 Configuration by Layer

- **Domain**: No configuration (pure logic)
- **Application**: Feature flags, timeouts
- **Infrastructure**: Database URL, connection pool size
- **Presentation**: Server port, CORS settings

### 9.2 Environment Variables

```bash
# Infrastructure
DATABASE_URL=postgres://...
DB_POOL_SIZE=10

# Presentation
SERVER_PORT=8080
LOG_LEVEL=info

# Application
REQUEST_TIMEOUT_SECS=30
```

---

## 10. See Also

- [API Standards](./api-standards.md) - HTTP endpoint conventions
- [Conventions](./conventions.md) - Naming and file structure rules
- [Constitution](./constitution.md) - Core project principles
- [System Overview](./system-overview.md) - Architecture at high level
- [Data Modeling](./data-modeling.md) - Entity design patterns
- [specs/001-ev-dashboard/plan.md](../../specs/001-ev-dashboard/plan.md) - Project architecture details
