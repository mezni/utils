# SYSTEM CONVENTIONS

**Last Updated**: 2026-06-23  
**Status**: Active  
**Authority**: Core Team

This document defines naming, file structure, and formatting conventions used throughout the BorneMap codebase.

---

## 1. Naming Conventions

### 1.1 API Resources

**Rule**: Use **plural nouns**, **lowercase**, **hyphens** for multi-word resources

✅ **Correct**:
```
/api/v1/partners
/api/v1/stations
/api/v1/chargers
/api/v1/charge-sessions
/api/v1/power-ratings
```

❌ **Incorrect**:
```
/api/v1/partner                    # singular
/api/v1/Partners                   # uppercase
/api/v1/partner_detail             # underscore
/api/v1/getPartner                 # verb prefix
```

### 1.2 Rust Code Naming

#### Modules (snake_case)

```rust
// Correct
mod domain_models;
mod infrastructure_database;
mod presentation_handlers;

// Incorrect
mod DomainModels;           // PascalCase
mod infrastructure-db;      // kebab-case
```

#### Types (PascalCase)

```rust
// Correct
struct Partner { }
enum Status { }
trait PartnerRepository { }
impl PartnerService { }

// Incorrect
struct partner { }          // snake_case
struct PARTNER { }          // SCREAMING_SNAKE_CASE
```

#### Constants (SCREAMING_SNAKE_CASE)

```rust
// Correct
const MAX_PARTNER_NAME_LENGTH: usize = 255;
const DEFAULT_PAGE_SIZE: u32 = 20;
const MIN_POWER_RATING_KW: f64 = 0.5;

// Incorrect
const maxPartnerNameLength: usize = 255;
const MaxPartnerNameLength: usize = 255;
```

#### Functions (snake_case)

```rust
// Correct
fn create_partner(name: String) -> Result<Partner, Error> { }
fn calculate_total_power() -> f64 { }
async fn fetch_partner(id: &PartnerId) -> Result<Option<Partner>, Error> { }

// Incorrect
fn CreatePartner(name: String) { }      // PascalCase
fn create-partner(name: String) { }     // kebab-case
fn createPartner(name: String) { }      // camelCase
```

#### Variables (snake_case)

```rust
// Correct
let partner_name = "ACME Corp";
let max_power_rating = 50.0;
let is_active = true;

// Incorrect
let partnerName = "ACME Corp";          // camelCase
let partner-name = "ACME Corp";         // kebab-case
let PartnerName = "ACME Corp";          // PascalCase
```

### 1.3 TypeScript/JavaScript Naming

#### Types/Interfaces (PascalCase)

```typescript
// Correct
interface Partner {
  id: string;
  name: string;
}

type Status = 'ACTIVE' | 'INACTIVE';

// Incorrect
interface partner { }
type status = 'ACTIVE';
```

#### Variables/Functions (camelCase)

```typescript
// Correct
const partnerName = "ACME Corp";
function createPartner(name: string): Partner { }
const maxPowerRating = 50;

// Incorrect
const PartnerName = "ACME Corp";
function create_partner(name: string) { }
const MAX_POWER_RATING = 50;
```

#### Constants (SCREAMING_SNAKE_CASE)

```typescript
// Correct
const MAX_PARTNER_NAME_LENGTH = 255;
const DEFAULT_PAGE_SIZE = 20;
const API_BASE_URL = "http://localhost:8080/api/v1";

// Incorrect
const maxPartnerNameLength = 255;
const MaxPartnerNameLength = 255;
```

### 1.4 Database Objects

#### Table Names (snake_case, singular)

```sql
-- Correct
CREATE TABLE ev.partner (
  id VARCHAR(16) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE'
);

CREATE TABLE ev.station (
  id VARCHAR(16) PRIMARY KEY,
  partner_id VARCHAR(16) REFERENCES ev.partner(id)
);

-- Incorrect
CREATE TABLE ev.partners { }     -- plural
CREATE TABLE ev.Partner { }      -- PascalCase
```

#### Column Names (snake_case)

```sql
-- Correct
CREATE TABLE ev.partner (
  id VARCHAR(16),
  partner_name VARCHAR(255),      -- descriptive
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  deleted_at TIMESTAMP NULL
);

-- Incorrect
CREATE TABLE ev.partner (
  id VARCHAR(16),
  name VARCHAR(255),              -- too generic
  createdAt TIMESTAMP,            -- camelCase
  Created TIMESTAMP               -- PascalCase
);
```

---

## 2. External ID Conventions

### 2.1 ID Format

**Format**: `PREFIX-<12-character-base62-identifier>`

| Entity | Prefix | Example |
|--------|--------|---------|
| **Partner** | `PRT` | `PRT-7f8g9h0i1j2k` |
| **Station** | `STA` | `STA-4a5b6c7d8e9f` |
| **Charger** | `CHR` | `CHR-1x2y3z4v5w6u` |

### 2.2 Base62 Alphabet

```
0-9 (10 characters)
a-z (26 characters)
A-Z (26 characters)
Total: 62 characters
```

✅ **Valid**: `abc123ABC`
❌ **Invalid**: `abc_123` (underscore not in Base62)
❌ **Invalid**: `ABC-123` (hyphens only for prefix separator)

### 2.3 Generation Rules

- IDs are **deterministic** (hash-based from seed string)
- NOT random
- Generation happens in **infrastructure layer only**
- Seed string: entity type + creation context
- Algorithm: nanoid with Base62 alphabet

```rust
// Example: Generate deterministic Partner ID
use nanoid::nanoid;

fn generate_partner_id(seed: &str) -> String {
    // Hash seed to create reproducible ID
    let hasher = blake3::hash(seed.as_bytes());
    let hash_str = hasher.to_hex().to_string();
    
    // Create deterministic ID from hash
    format!("PRT-{}", &hash_str[0..12])  // Take first 12 chars
}
```

### 2.4 ID Uniqueness

- IDs MUST be unique within entity type
- Database enforces uniqueness via PRIMARY KEY
- Client cannot predict IDs (opaque to caller)
- Never expose internal numeric IDs

---

## 3. File Structure Conventions

### 3.1 Rust File Organization

```
services/admin-service/src/
├── main.rs                           # App entry point
├── lib.rs                            # Library exports
│
├── domain/                           # Pure business logic
│   ├── mod.rs
│   ├── entities.rs                   # Entity definitions
│   ├── value_objects.rs              # Value objects (Status, etc.)
│   ├── repositories.rs               # Repository traits
│   └── errors.rs                     # Domain errors
│
├── application/                      # Use-case orchestration
│   ├── mod.rs
│   ├── services/
│   │   ├── mod.rs
│   │   ├── create_partner.rs
│   │   ├── update_partner.rs
│   │   └── delete_partner.rs
│   ├── dto.rs                        # Data transfer objects
│   └── errors.rs                     # Application errors
│
├── infrastructure/                   # External integration
│   ├── mod.rs
│   ├── database/
│   │   ├── mod.rs
│   │   ├── postgres.rs
│   │   ├── migrations/
│   │   │   ├── 001_create_partner.sql
│   │   │   ├── 002_create_station.sql
│   │   │   └── 003_create_charger.sql
│   │   └── repositories/
│   │       ├── mod.rs
│   │       ├── partner_repository.rs
│   │       ├── station_repository.rs
│   │       └── charger_repository.rs
│   ├── config.rs                     # Configuration
│   └── errors.rs                     # Infrastructure errors
│
├── presentation/                     # HTTP handling
│   ├── mod.rs
│   ├── handlers/
│   │   ├── mod.rs
│   │   ├── partner_handler.rs
│   │   ├── station_handler.rs
│   │   └── charger_handler.rs
│   ├── response.rs                   # Standard response format
│   ├── error_handler.rs              # Error mapping to HTTP
│   └── middleware/
│       ├── mod.rs
│       ├── logging.rs
│       └── error_handling.rs
│
└── tests/                            # Integration tests
    ├── integration/
    │   ├── mod.rs
    │   └── partner_api.rs
    └── fixtures/
        └── test_data.rs
```

### 3.2 React File Organization

```
apps/admin-dashboard/src/
├── main.tsx                          # App entry point
├── App.tsx                           # Root component
├── App.css                           # Root styles
│
├── domains/                          # Feature domains
│   ├── partners/
│   │   ├── components/
│   │   │   ├── PartnerList.tsx
│   │   │   ├── PartnerForm.tsx
│   │   │   └── PartnerDetail.tsx
│   │   ├── hooks/
│   │   │   ├── usePartnerList.ts
│   │   │   └── usePartnerForm.ts
│   │   ├── services/
│   │   │   └── partnerApi.ts        # API calls
│   │   ├── types.ts                 # Domain types
│   │   └── index.ts                 # Barrel export
│   ├── stations/
│   │   └── ...
│   └── chargers/
│       └── ...
│
├── shared/                           # Shared across domains
│   ├── components/
│   │   ├── Header.tsx
│   │   ├── Sidebar.tsx
│   │   └── Button.tsx
│   ├── hooks/
│   │   ├── useApi.ts                # HTTP client hook
│   │   └── useAuth.ts               # Auth state
│   ├── services/
│   │   ├── api.ts                   # HTTP client setup
│   │   └── error.ts                 # Error handling
│   ├── types/
│   │   ├── api.ts                   # API response types
│   │   └── common.ts                # Common types
│   └── styles/
│       ├── index.css
│       └── variables.css
│
└── __tests__/                        # Test files
    ├── components/
    └── hooks/
```

### 3.3 File Naming Rules

**Rust**:
- ✅ `partner_handler.rs`
- ✅ `create_partner_use_case.rs`
- ✅ `partner_repository.rs`

**TypeScript/React**:
- ✅ `PartnerList.tsx` (component, PascalCase)
- ✅ `usePartnerList.ts` (hook, camelCase)
- ✅ `partnerApi.ts` (service, camelCase)
- ✅ `partner.types.ts` (types, lowercase + .types)

---

## 4. API Conventions

### 4.1 Endpoint Paths

**Pattern**: `/api/v1/<resource>` or `/api/v1/<resource>/<id>`

```
GET    /api/v1/partners              # List all
GET    /api/v1/partners/PRT-123      # Get one
POST   /api/v1/partners              # Create
PATCH  /api/v1/partners/PRT-123      # Update
DELETE /api/v1/partners/PRT-123      # Delete

GET    /api/v1/stations?partner=PRT-123      # Filter
GET    /api/v1/partners?page=1&limit=20      # Paginate
```

### 4.2 Request/Response Fields

**Database** (snake_case):
```sql
SELECT id, partner_name, created_at FROM partner;
```

**API/JSON** (camelCase):
```json
{
  "id": "PRT-123",
  "partnerName": "ACME Corp",
  "createdAt": "2026-06-23T10:30:00Z"
}
```

**Mapping happens in DTO layer** (application layer converts)

---

## 5. Code Style Conventions

### 5.1 Line Length

- **Rust**: Max 100 characters
- **TypeScript**: Max 100 characters
- **SQL**: Max 100 characters

```rust
// Too long (over 100 chars)
let partner = Partner::new("Very Long Name That Exceeds Limit".to_string(), "email@very-long-domain.com".to_string());

// Better
let partner = Partner::new(
    "Very Long Name That Exceeds Limit".to_string(),
    "email@very-long-domain.com".to_string(),
)?;
```

### 5.2 Indentation

- **Rust**: 4 spaces
- **TypeScript**: 2 spaces
- **SQL**: 2 spaces
- **JSON**: 2 spaces

```rust
// Rust: 4 spaces
struct Partner {
    id: PartnerId,
    name: String,
    email: String,
}

impl Partner {
    fn new(name: String, email: String) -> Result<Self, Error> {
        Ok(Partner {
            id: generate_id(),
            name,
            email,
        })
    }
}
```

### 5.3 Comments

**Rule**: Explain WHY, not WHAT

```rust
// Bad: Explains what, not why
let status = Status::Active;  // Set status to Active

// Good: Explains the reasoning
// Default to Active so new partners are immediately operational
let status = Status::Active;

// Bad: Unnecessary comment
let name = partner.name;  // Get the partner name

// Good: Only comment when non-obvious
// Cache partner name to reduce database lookups in loop
let name = partner.name;
```

### 5.4 Doc Comments

**Rule**: Public APIs MUST have doc comments

```rust
/// Creates a new Partner with the given name and email.
///
/// # Arguments
/// * `name` - Partner name (max 255 characters)
/// * `email` - Valid email address
///
/// # Returns
/// Ok(Partner) on success, Err(DomainError) if validation fails
///
/// # Example
/// ```rust
/// let partner = Partner::new("ACME Corp".to_string(), "contact@acme.com".to_string())?;
/// assert_eq!(partner.status, Status::Active);
/// ```
pub fn new(name: String, email: String) -> Result<Self, DomainError> {
    validate_email(&email)?;
    Ok(Partner {
        id: generate_id(),
        name,
        email,
        status: Status::Active,
    })
}
```

---

## 6. Layer Conventions

### 6.1 Responsibility by Layer

| Layer | Module Suffix | Naming Pattern | Exports |
|-------|---------------|----------------|---------|
| **Domain** | - | `Partner`, `Station` | Types, traits, errors |
| **Application** | `Service` | `CreatePartnerService` | Services, DTOs |
| **Infrastructure** | `Repository` | `PostgresPartnerRepository` | Implementations |
| **Presentation** | `Handler` | `partner_handler` | HTTP handlers |

### 6.2 Imports by Layer

**In Domain**:
```rust
use std::...;  // OK
use crate::domain::...;  // OK

// NOT OK:
use crate::application::...;
use crate::infrastructure::...;
```

**In Application**:
```rust
use crate::domain::...;  // OK
use crate::application::...;  // OK
use tokio::...;  // OK (async runtime)

// NOT OK:
use sqlx::...;  // Database in app
use actix_web::...;  // Framework in app
```

**In Infrastructure**:
```rust
use crate::domain::...;  // OK
use crate::infrastructure::...;  // OK
use sqlx::...;  // OK
use tokio::...;  // OK

// NOT OK:
use actix_web::...;  // Framework not in infra
```

**In Presentation**:
```rust
use crate::*;  // All layers OK
use actix_web::...;  // OK (framework layer)
use serde::...;  // OK (serialization)
```

---

## 7. Testing Conventions

### 7.1 Test File Placement

```
src/
├── domain/
│   ├── entities.rs
│   └── entities_tests.rs        # or #[cfg(test)] module
├── application/
│   ├── services/
│   │   ├── create_partner.rs
│   │   └── create_partner_tests.rs
└── infrastructure/
    ├── database/
    │   └── partner_repository.rs

tests/
└── integration/
    └── partner_api_test.rs
```

### 7.2 Test Naming

```rust
// Unit test: test_<function>_<scenario>
#[test]
fn test_partner_new_valid_input() { }

#[test]
fn test_partner_new_invalid_email() { }

// Async test: #[tokio::test]
#[tokio::test]
async fn test_create_partner_use_case_success() { }
```

### 7.3 Assertion Style

```rust
// Readable assertions
assert_eq!(partner.status, Status::Active);
assert!(partner.is_valid());
assert!(!partner.is_deleted());

// With messages
assert_eq!(result.unwrap_err().code, "INVALID_INPUT", 
    "Expected validation error for invalid email");
```

---

## 8. Checklist for New Code

- ✅ Names follow convention (snake_case/PascalCase/camelCase)
- ✅ Files follow organization pattern
- ✅ IDs follow prefix format (PRT-/STA-/CHR-)
- ✅ API endpoints use plural nouns
- ✅ Layer dependencies respect Clean Architecture
- ✅ Public functions have doc comments
- ✅ Comments explain WHY, not WHAT
- ✅ Test names clearly describe scenario
- ✅ Line lengths under 100 characters
- ✅ Indentation matches language standard

---

## 9. See Also

- [Architecture](./architecture.md) - Layer responsibilities
- [API Standards](./api-standards.md) - HTTP endpoint conventions
- [Constitution](./constitution.md) - Core principles
- [Data Modeling](./data-modeling.md) - Entity design patterns
