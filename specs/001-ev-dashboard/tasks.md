# Tasks: EV Dashboard Platform Kernel

**Input**: Design documents from `/specs/001-ev-dashboard/`
**Status**: ✅ All constitution checks passed
**Tests**: Tests are REQUIRED per constitution (TDD approach)

---

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1, US2, US3, US4)
- Include exact file paths

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Project initialization and basic structure

- [X] T001 Create project structure per implementation plan
  - Create services/admin-service/ with src/, Cargo.toml, .env
  - Create apps/admin-dashboard/ with src/, package.json, vite.config.ts
  - Create crates/platform-core/ with src/, Cargo.toml
  - Create crates/platform-db/ with src/, Cargo.toml
  - Create infrastructure/ directory structure
- [X] T002 Initialize Rust workspace at repository root with Cargo.toml
  - Add services/admin-service/ as workspace member
  - Add crates/platform-core/ as workspace member
  - Add crates/platform-db/ as workspace member
- [X] T003 [P] Initialize React frontend project with Vite + TypeScript
  - Create apps/admin-dashboard/src/pages/
  - Create apps/admin-dashboard/src/features/
  - Create apps/admin-dashboard/src/components/
  - Create apps/admin-dashboard/src/api/
  - Create apps/admin-dashboard/src/hooks/
  - Create apps/admin-dashboard/src/types/
  - Configure TailwindCSS
  - Configure React Router v6
- [X] T004 [P] Setup platform-core crate structure
  - Create platform-core/src/error/
  - Create platform-core/src/result/
  - Create platform-core/src/config/
  - Create platform-core/src/id/
  - Create platform-core/src/validation/
  - Add rust-nanoid dependency for deterministic ID generation
- [X] T005 [P] Setup platform-db crate structure
  - Create platform-db/src/pool/
  - Create platform-db/src/migration/
  - Create platform-db/src/transaction/
  - Add sqlx dependency and configuration
- [X] T006 [P] Configure Docker Compose infrastructure
  - Create infrastructure/docker/postgres/Dockerfile
  - Create infrastructure/docker/admin-service/Dockerfile
  - Create infrastructure/docker/admin-dashboard/Dockerfile
  - Create infrastructure/postgres/init/ directory
  - Create docker-compose.yml
- [X] T007 [P] Setup linting and formatting tools
  - Add rustfmt and clippy to backend
  - Add ESLint and Prettier to frontend
  - Configure .editorconfig

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

- [ ] T008 Create database schema and migrations framework
  - Create services/admin-service/migrations/ directory
  - Create migration 202606230001_init_schema.sql
  - Create migration 202606230002_create_partners.sql
  - Create migration 202606230003_create_stations.sql
  - Create migration 202606230004_create_chargers.sql
  - Configure SQLx migration runner in platform-db
- [ ] T009 [P] Implement deterministic ID generation in platform-core
  - Create platform-core/src/id/mod.rs
  - Implement generate_partner_id(seed: &str) -> String
  - Implement generate_station_id(seed: &str) -> String
  - Implement generate_charger_id(seed: &str) -> String
  - Implement validate_partner_id(id: &str) -> bool
  - Implement validate_station_id(id: &str) -> bool
  - Implement validate_charger_id(id: &str) -> bool
- [ ] T010 [P] Setup SQLx pool and database connection in platform-db
  - Create platform-db/src/pool/postgres_pool.rs
  - Create platform-db/src/pool/mod.rs
  - Implement PgPool::new() from connection string
  - Add connection pool configuration
- [ ] T011 [P] Setup API routing and middleware structure in admin-service
  - Create admin-service/src/presentation/routes/ directory
  - Create admin-service/src/presentation/handlers/ directory
  - Create admin-service/src/presentation/middleware/ directory
  - Create admin-service/src/presentation/requests/ directory
  - Create admin-service/src/presentation/responses/ directory
- [ ] T012 [P] Create base error system and result types in platform-core
  - Create platform-core/src/error/mod.rs
  - Create platform-core/src/result/mod.rs
  - Define AppError enum with validation, not_found, internal_error variants
  - Define AppResult<T> type alias
  - Add serde serialization support
- [ ] T013 [P] Create base config management in platform-core
  - Create platform-core/src/config/mod.rs
  - Create platform-core/src/config/app_config.rs
  - Create platform-core/src/config/database_config.rs
  - Create platform-core/src/config/server_config.rs
- [ ] T014 [P] Create database models for all entities
  - Create platform-core/src/models/partner.rs
  - Create platform-core/src/models/station.rs
  - Create platform-core/src/models/charger.rs
  - Implement validate() method for each entity
  - Implement is_active() method for each entity
  - Add chrono dependencies for timestamps
- [ ] T015 [P] Create status value object in platform-core
  - Create platform-core/src/value_objects/status.rs
  - Define Status enum (ACTIVE, INACTIVE, MAINTENANCE, DISABLED)
  - Implement status validation logic
  - Add validation method
- [ ] T016 [P] Create pagination utilities
  - Create platform-core/src/pagination/mod.rs
  - Create platform-core/src/pagination/pagination.rs
  - Implement Pagination struct with page, limit, offset
  - Implement paginate<T>() method for Vec<T>
  - Add total count support

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - View Dashboard Overview (Priority: P1) 🎯 MVP

**Goal**: Display KPI metrics showing total counts of partners, stations, and chargers (only active records)

**Independent Test**: Access dashboard endpoint and verify KPI metrics accurately reflect database counts of active records (deleted_at IS NULL)

### Tests for User Story 1 (REQUIRED) ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T017 [US1] Unit test for dashboard KPIs query in services/admin-service/src/tests/dashboard_kpis_test.rs
- [ ] T018 [US1] Integration test for GET /api/v1/dashboard/kpis endpoint in services/admin-service/src/tests/integration/test_dashboard_kpis.rs

### Implementation for User Story 1

- [ ] T019 [P] [US1] Create Partner entity in platform-core/src/models/partner.rs (T014 is prerequisite)
- [ ] T020 [P] [US1] Create Station entity in platform-core/src/models/station.rs (T014 is prerequisite)
- [ ] T021 [P] [US1] Create Charger entity in platform-core/src/models/charger.rs (T014 is prerequisite)
- [ ] T022 [US1] Implement PartnerRepository trait in domain layer
  - Create admin-service/src/domain/repositories/partner_repository.rs
  - Define create(), get_by_id(), list(), hard_delete(), soft_delete(), undelete() methods
  - Add deleted_at filtering in list() and get_by_id() methods
- [ ] T023 [US1] Implement StationRepository trait in domain layer
  - Create admin-service/src/domain/repositories/station_repository.rs
  - Define create(), get_by_id(), list(), hard_delete(), soft_delete(), undelete() methods
- [ ] T024 [US1] Implement ChargerRepository trait in domain layer
  - Create admin-service/src/domain/repositories/charger_repository.rs
  - Define create(), get_by_id(), list(), update_status(), hard_delete(), soft_delete(), undelete() methods
- [ ] T025 [US1] Implement PartnerRepositoryImpl in infrastructure layer
  - Create admin-service/src/infrastructure/persistence/sqlx/partner_repository_impl.rs
  - Implement deterministic ID generation using seed from created_by field
  - Implement create() with validation
  - Implement get_by_id() with deleted_at IS NULL filter
  - Implement list() with deleted_at IS NULL filter and pagination
  - Implement hard_delete() with CASCADE to stations
  - Implement soft_delete() without CASCADE
  - Implement undelete() by setting deleted_at = NULL
- [ ] T026 [US1] Implement StationRepositoryImpl in infrastructure layer
  - Create admin-service/src/infrastructure/persistence/sqlx/station_repository_impl.rs
  - Implement deterministic ID generation using seed from created_by field
  - Implement create() with partner_id validation
  - Implement get_by_id() with deleted_at IS NULL filter
  - Implement list() with deleted_at IS NULL filter and pagination
  - Implement hard_delete() with CASCADE to chargers
  - Implement soft_delete() without CASCADE
  - Implement undelete() by setting deleted_at = NULL
- [ ] T027 [US1] Implement ChargerRepositoryImpl in infrastructure layer
  - Create admin-service/src/infrastructure/persistence/sqlx/charger_repository_impl.rs
  - Implement deterministic ID generation using seed from created_by field
  - Implement create() with station_id validation
  - Implement get_by_id() with deleted_at IS NULL filter
  - Implement list() with deleted_at IS NULL filter and pagination
  - Implement update_status()
  - Implement hard_delete() without CASCADE
  - Implement soft_delete() without CASCADE
  - Implement undelete() by setting deleted_at = NULL
- [ ] T028 [US1] Implement PartnerService in application layer
  - Create admin-service/src/application/services/partner_service.rs
  - Create create() method with business logic validation
  - Create get_by_id() method
  - Create list() method
  - Create hard_delete() method
  - Create soft_delete() method
  - Create undelete() method
- [ ] T029 [US1] Implement StationService in application layer
  - Create admin-service/src/application/services/station_service.rs
  - Create create() method with partner_id validation
  - Create get_by_id() method
  - Create list() method
  - Create hard_delete() method
  - Create soft_delete() method
  - Create undelete() method
- [ ] T030 [US1] Implement ChargerService in application layer
  - Create admin-service/src/application/services/charger_service.rs
  - Create create() method with station_id validation
  - Create get_by_id() method
  - Create list() method
  - Create update_status() method
  - Create hard_delete() method
  - Create soft_delete() method
  - Create undelete() method
- [ ] T031 [US1] Create DashboardHandler in presentation layer
  - Create admin-service/src/presentation/handlers/dashboard_handler.rs
  - Implement GET /api/v1/dashboard/kpis endpoint
  - Execute SQL queries to count active partners, stations, and chargers
  - Return standardized response format
- [ ] T032 [US1] Create PartnerHandler in presentation layer
  - Create admin-service/src/presentation/handlers/partner_handler.rs
  - Implement GET /api/v1/partners endpoint
  - Implement POST /api/v1/partners endpoint
  - Implement GET /api/v1/partners/{id} endpoint
  - Implement DELETE /api/v1/partners/{id} endpoint (hard delete)
  - Implement PUT /api/v1/partners/{id} endpoint (soft delete/undelete)
- [ ] T033 [US1] Create StationHandler in presentation layer
  - Create admin-service/src/presentation/handlers/station_handler.rs
  - Implement GET /api/v1/stations endpoint
  - Implement POST /api/v1/stations endpoint
  - Implement GET /api/v1/stations/{id} endpoint
  - Implement DELETE /api/v1/stations/{id} endpoint (hard delete)
  - Implement PUT /api/v1/stations/{id} endpoint (soft delete/undelete)
- [ ] T034 [US1] Create ChargerHandler in presentation layer
  - Create admin-service/src/presentation/handlers/charger_handler.rs
  - Implement GET /api/v1/chargers endpoint
  - Implement POST /api/v1/chargers endpoint
  - Implement GET /api/v1/chargers/{id} endpoint
  - Implement DELETE /api/v1/chargers/{id} endpoint (hard delete)
  - Implement PUT /api/v1/chargers/{id} endpoint (soft delete/undelete)
- [ ] T035 [US1] Register API routes in admin-service
  - Create admin-service/src/presentation/routes/mod.rs
  - Create admin-service/src/presentation/routes/dashboard_routes.rs
  - Create admin-service/src/presentation/routes/partner_routes.rs
  - Create admin-service/src/presentation/routes/station_routes.rs
  - Create admin-service/src/presentation/routes/charger_routes.rs
  - Register all routes with Actix-Web
- [ ] T036 [US1] Add request middleware and logging
  - Create admin-service/src/presentation/middleware/request_id.rs
  - Create admin-service/src/presentation/middleware/logging.rs
  - Create admin-service/src/presentation/middleware/error_mapper.rs
  - Apply middleware to all routes
- [ ] T037 [US1] Create PartnerRequest/Response DTOs
  - Create admin-service/src/presentation/requests/create_partner_request.rs
  - Create admin-service/src/presentation/responses/partner_response.rs
  - Create partner_response_from_entity() mapper
- [ ] T038 [US1] Create StationRequest/Response DTOs
  - Create admin-service/src/presentation/requests/create_station_request.rs
  - Create admin-service/src/presentation/responses/station_response.rs
  - Create station_response_from_entity() mapper
- [ ] T039 [US1] Create ChargerRequest/Response DTOs
  - Create admin-service/src/presentation/requests/create_charger_request.rs
  - Create admin-service/src/presentation/responses/charger_response.rs
  - Create charger_response_from_entity() mapper
- [ ] T040 [US1] Implement database migrations
  - Create migration 202606230001_init_schema.sql with ev schema creation
  - Create migration 202606230002_create_partners.sql with partners table, constraints, indexes, views
  - Create migration 202606230003_create_stations.sql with stations table, constraints, indexes, views
  - Create migration 202606230004_create_chargers.sql with chargers table, constraints, indexes, views
  - Add created_by and updated_by FK constraints to all tables (assuming admins table exists)
- [ ] T041 [US1] Implement database pool initialization in admin-service
  - Create admin-service/src/db/mod.rs
  - Create admin-service/src/db/postgres_pool.rs
  - Initialize SQLx pool in main.rs
- [ ] T042 [US1] Create PartnerResponseFromEntity mapper in infrastructure
  - Create admin-service/src/infrastructure/persistence/mappers/partner_mapper.rs
  - Implement partner_record_to_entity() method
  - Implement entity_to_partner_response() method
- [ ] T043 [US1] Create StationResponseFromEntity mapper in infrastructure
  - Create admin-service/src/infrastructure/persistence/mappers/station_mapper.rs
  - Implement station_record_to_entity() method
  - Implement entity_to_station_response() method
- [ ] T044 [US1] Create ChargerResponseFromEntity mapper in infrastructure
  - Create admin-service/src/infrastructure/persistence/mappers/charger_mapper.rs
  - Implement charger_record_to_entity() method
  - Implement entity_to_charger_response() method
- [ ] T045 [US1] Implement error handling and validation throughout
  - Add validation for all input fields
  - Add error handling for database errors
  - Add error handling for validation errors
  - Return standardized error responses

**Checkpoint**: At this point, User Story 1 should be fully functional and testable independently

---

## Phase 4: User Story 2 - Manage Partners (Priority: P2)

**Goal**: Create, view, and manage partner organizations (EV network operators)

**Independent Test**: Create a partner via API, verify it appears in database with correct deterministic ID format, ensure partner count updates

### Tests for User Story 2 (REQUIRED) ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T046 [P] [US2] Unit test for PartnerService.create() in services/admin-service/src/tests/partner_service_create_test.rs
- [ ] T047 [P] [US2] Unit test for PartnerService.get_by_id() in services/admin-service/src/tests/partner_service_get_test.rs
- [ ] T048 [P] [US2] Unit test for PartnerService.list() in services/admin-service/src/tests/partner_service_list_test.rs
- [ ] T049 [P] [US2] Unit test for PartnerService.hard_delete() in services/admin-service/src/tests/partner_service_hard_delete_test.rs
- [ ] T050 [P] [US2] Unit test for PartnerService.soft_delete() in services/admin-service/src/tests/partner_service_soft_delete_test.rs
- [ ] T051 [P] [US2] Unit test for PartnerService.undelete() in services/admin-service/src/tests/partner_service_undelete_test.rs
- [ ] T052 [P] [US2] Integration test for Partner CRUD operations in services/admin-service/src/tests/integration/test_partner_crud.rs

### Implementation for User Story 2

- [ ] T053 [P] [US2] Create Partner entity with all fields in platform-core/src/models/partner.rs (T019 is prerequisite)
  - Add status field
  - Add is_valid field
  - Add created_by and updated_by fields
  - Add deleted_at field
  - Implement validate() method with status enum validation
- [ ] T054 [P] [US2] Update PartnerResponse DTO in admin-service/src/presentation/responses/partner_response.rs (T037 is prerequisite)
  - Add status field
  - Add is_valid field
  - Add created_by field
  - Add updated_by field
  - Add created_at and updated_at fields
  - Add deleted_at field (nullable)
- [ ] T055 [P] [US2] Update PartnerRequest DTO in admin-service/src/presentation/requests/create_partner_request.rs (T037 is prerequisite)
  - Add status field (optional, default ACTIVE)
  - Add is_valid field (optional, default TRUE)
- [ ] T056 [US2] Enhance PartnerRepository with status field handling (T022 is prerequisite)
  - Update list() method to support status filter
  - Update get_by_id() to handle status validation
- [ ] T057 [US2] Enhance PartnerRepositoryImpl with status field handling (T025 is prerequisite)
  - Update create() to include status and is_valid
  - Update get_by_id() to validate status
  - Update list() to support status filtering
- [ ] T058 [US2] Enhance PartnerService with status field handling (T028 is prerequisite)
  - Add status validation in create()
  - Add is_valid field handling
  - Update validate() method to check status enum
- [ ] T059 [US2] Enhance PartnerHandler with status field support (T032 is prerequisite)
  - Update POST /api/v1/partners to accept status and is_valid
  - Update GET /api/v1/partners to return status and is_valid
  - Update DELETE /api/v1/partners to enforce hard delete with CASCADE
  - Update PUT /api/v1/partners/{id} to support soft delete and undelete
- [ ] T060 [US2] Add duplicate name validation in PartnerService (T028 is prerequisite)
  - Check if partner name already exists in create() method
  - Return validation error if name is not unique
- [ ] T061 [US2] Add undelete method to PartnerHandler (T032 is prerequisite)
  - Implement PUT /api/v1/partners/{id} for undelete operation
  - Validate partner is soft-deleted
  - Call PartnerService.undelete()
- [ ] T062 [US2] Update database migration for partners table status fields (T040 is prerequisite)
  - Add status column (TEXT, default 'ACTIVE')
  - Add is_valid column (BOOLEAN, default TRUE)
  - Add constraint for status enum values
  - Add unique constraint on name column

**Checkpoint**: At this point, User Stories 1 AND 2 should both work independently

---

## Phase 5: User Story 3 - Manage Stations (Priority: P3)

**Goal**: Create, view, and manage charging stations linked to partner organizations

**Independent Test**: Create a station with existing active partner, verify deterministic STA ID format, ensure partner's station count updates

### Tests for User Story 3 (REQUIRED) ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T063 [P] [US3] Unit test for StationService.create() in services/admin-service/src/tests/station_service_create_test.rs
- [ ] T064 [P] [US3] Unit test for StationService.get_by_id() in services/admin-service/src/tests/station_service_get_test.rs
- [ ] T065 [P] [US3] Unit test for StationService.list() in services/admin-service/src/tests/station_service_list_test.rs
- [ ] T066 [P] [US3] Unit test for StationService.hard_delete() in services/admin-service/src/tests/station_service_hard_delete_test.rs
- [ ] T067 [P] [US3] Unit test for StationService.soft_delete() in services/admin-service/src/tests/station_service_soft_delete_test.rs
- [ ] T068 [P] [US3] Unit test for StationService.undelete() in services/admin-service/src/tests/station_service_undelete_test.rs
- [ ] T069 [P] [US3] Integration test for Station CRUD operations in services/admin-service/src/tests/integration/test_station_crud.rs

### Implementation for User Story 3

- [ ] T070 [P] [US3] Create Station entity with all fields in platform-core/src/models/station.rs (T020 is prerequisite)
  - Add status field
  - Add location field
  - Add created_by and updated_by fields
  - Add deleted_at field
  - Implement validate() method with status enum validation
  - Validate partner_id format (PRT-*)
- [ ] T071 [P] [US3] Update StationResponse DTO in admin-service/src/presentation/responses/station_response.rs (T038 is prerequisite)
  - Add status field
  - Add location field
  - Add created_by field
  - Add updated_by field
  - Add created_at and updated_at fields
  - Add deleted_at field (nullable)
- [ ] T072 [P] [US3] Update StationRequest DTO in admin-service/src/presentation/requests/create_station_request.rs (T038 is prerequisite)
  - Add status field (optional, default ACTIVE)
  - Add location field (optional)
- [ ] T073 [US3] Enhance StationRepository with status and location field handling (T023 is prerequisite)
  - Update create() to include status and location
  - Update get_by_id() to validate status
  - Update list() to support status filtering
  - Update list() to support partner_id filtering
- [ ] T074 [US3] Enhance StationRepositoryImpl with status and location field handling (T026 is prerequisite)
  - Update create() to include status and location
  - Update get_by_id() to validate status
  - Update list() to support status and partner_id filtering
- [ ] T075 [US3] Enhance StationService with status and location field handling (T029 is prerequisite)
  - Add status validation in create()
  - Add location field handling
  - Add location validation (1-200 characters)
  - Update validate() method to check status enum and location format
- [ ] T076 [US3] Enhance StationHandler with status and location field support (T033 is prerequisite)
  - Update POST /api/v1/stations to accept status and location
  - Update GET /api/v1/stations to return status and location
  - Update DELETE /api/v1/stations to enforce hard delete with CASCADE
  - Update PUT /api/v1/stations/{id} to support soft delete and undelete
- [ ] T077 [US3] Add partner_id validation in StationService (T029 is prerequisite)
  - Check if partner exists and is active in create() method
  - Return validation error if partner is not found or soft-deleted
- [ ] T078 [US3] Add undelete method to StationHandler (T033 is prerequisite)
  - Implement PUT /api/v1/stations/{id} for undelete operation
  - Validate station is soft-deleted
  - Call StationService.undelete()
- [ ] T079 [US3] Update database migration for stations table status and location fields (T040 is prerequisite)
  - Add status column (TEXT, default 'ACTIVE')
  - Add location column (TEXT, nullable)
  - Add constraint for status enum values
  - Add unique constraint on name column
  - Add constraint to validate partner_id format (PRT-*)
  - Add constraint to ensure partner_id exists and is active

**Checkpoint**: All user stories should now be independently functional

---

## Phase 6: User Story 4 - Manage Chargers (Priority: P4)

**Goal**: Create, view, and manage charging units within stations

**Independent Test**: Create a charger with existing active station, verify deterministic CHR ID format, check station has correct charger count

### Tests for User Story 4 (REQUIRED) ⚠️

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T080 [P] [US4] Unit test for ChargerService.create() in services/admin-service/src/tests/charger_service_create_test.rs
- [ ] T081 [P] [US4] Unit test for ChargerService.get_by_id() in services/admin-service/src/tests/charger_service_get_test.rs
- [ ] T082 [P] [US4] Unit test for ChargerService.list() in services/admin-service/src/tests/charger_service_list_test.rs
- [ ] T083 [P] [US4] Unit test for ChargerService.update_status() in services/admin-service/src/tests/charger_service_update_status_test.rs
- [ ] T084 [P] [US4] Unit test for ChargerService.hard_delete() in services/admin-service/src/tests/charger_service_hard_delete_test.rs
- [ ] T085 [P] [US4] Unit test for ChargerService.soft_delete() in services/admin-service/src/tests/charger_service_soft_delete_test.rs
- [ ] T086 [P] [US4] Unit test for ChargerService.undelete() in services/admin-service/src/tests/charger_service_undelete_test.rs
- [ ] T087 [P] [US4] Integration test for Charger CRUD operations in services/admin-service/src/tests/integration/test_charger_crud.rs

### Implementation for User Story 4

- [ ] T088 [P] [US4] Create Charger entity with all fields in platform-core/src/models/charger.rs (T021 is prerequisite)
  - Add status field
  - Add power_rating field
  - Add created_by and updated_by fields
  - Add deleted_at field
  - Implement validate() method with status enum validation
  - Validate station_id format (STA-*)
  - Validate power_rating range (1-1000 kW)
- [ ] T089 [P] [US4] Update ChargerResponse DTO in admin-service/src/presentation/responses/charger_response.rs (T039 is prerequisite)
  - Add status field
  - Add power_rating field (in kW)
  - Add created_by field
  - Add updated_by field
  - Add created_at and updated_at fields
  - Add deleted_at field (nullable)
- [ ] T090 [P] [US4] Update ChargerRequest DTO in admin-service/src/presentation/requests/create_charger_request.rs (T039 is prerequisite)
  - Add status field (optional, default ACTIVE)
  - Add power_rating field (optional, default 50)
- [ ] T091 [US4] Enhance ChargerRepository with status and power_rating field handling (T024 is prerequisite)
  - Update create() to include status and power_rating
  - Update get_by_id() to validate status
  - Update list() to support status filtering
  - Update list() to support station_id filtering
  - Add update_status() method
- [ ] T092 [US4] Enhance ChargerRepositoryImpl with status and power_rating field handling (T027 is prerequisite)
  - Update create() to include status and power_rating
  - Update get_by_id() to validate status
  - Update list() to support status and station_id filtering
  - Add update_status() implementation
- [ ] T093 [US4] Enhance ChargerService with status and power_rating field handling (T030 is prerequisite)
  - Add status validation in create()
  - Add power_rating field handling
  - Add power_rating validation (1-1000 kW)
  - Add status enum validation
  - Update validate() method to check status enum and power_rating range
- [ ] T094 [US4] Enhance ChargerHandler with status and power_rating field support (T034 is prerequisite)
  - Update POST /api/v1/chargers to accept status and power_rating
  - Update GET /api/v1/chargers to return status and power_rating
  - Update PUT /api/v1/chargers/{id} to support status update, soft delete, and undelete
  - Implement PUT /api/v1/chargers/{id} for status update
- [ ] T095 [US4] Add station_id validation in ChargerService (T030 is prerequisite)
  - Check if station exists and is active in create() method
  - Return validation error if station is not found or soft-deleted
- [ ] T096 [US4] Add undelete method to ChargerHandler (T034 is prerequisite)
  - Implement PUT /api/v1/chargers/{id} for undelete operation
  - Validate charger is soft-deleted
  - Call ChargerService.undelete()
- [ ] T097 [US4] Update database migration for chargers table status and power_rating fields (T040 is prerequisite)
  - Add status column (TEXT, default 'ACTIVE')
  - Add power_rating column (INTEGER, range 1-1000)
  - Add constraint for status enum values
  - Add constraint to validate station_id format (STA-*)
  - Add constraint to ensure station_id exists and is active
  - Add constraint for power_rating range (1-1000)

**Checkpoint**: All user stories should now be independently functional

---

## Phase 7: Polish & Cross-Cutting Concerns

**Purpose**: Improvements that affect multiple user stories

- [ ] T098 [P] Documentation updates in docs/
  - Update README.md with project structure and setup instructions
  - Update docs/core/architecture.md with implementation details
  - Update docs/core/api-standards.md with endpoint documentation
  - Update docs/epics/E001-dashboard-core/epic.md with status field documentation
- [ ] T099 Code cleanup and refactoring
  - Apply rustfmt and clippy fixes to backend code
  - Apply ESLint and Prettier fixes to frontend code
  - Refactor code for better readability and maintainability
- [ ] T100 Performance optimization across all stories
  - Add database indexes for frequently queried columns
  - Optimize SQL queries for list operations
  - Add pagination for all list endpoints
  - Optimize response serialization
- [ ] T101 [P] Additional unit tests (if needed) in tests/unit/
  - Add unit tests for shared utilities
  - Add unit tests for domain invariants
  - Increase test coverage to meet targets
- [ ] T102 Security hardening
  - Add input sanitization for all user inputs
  - Add SQL injection protection (already handled by SQLx)
  - Add rate limiting for API endpoints
  - Add CORS configuration
- [ ] T103 Run quickstart.md validation
  - Test all quickstart steps end-to-end
  - Verify all API endpoints work correctly
  - Verify all database migrations apply successfully
  - Verify frontend can connect to backend

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup completion - BLOCKS all user stories
- **User Stories (Phase 3-6)**: All depend on Foundational phase completion
  - User stories can then proceed in parallel (if staffed)
  - Or sequentially in priority order (P1 → P2 → P3 → P4)
- **Polish (Phase 7)**: Depends on all desired user stories being complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational (Phase 2) - No dependencies on other stories
- **User Story 2 (P2)**: Can start after Foundational (Phase 2) - Integrates with US1 but independently testable
- **User Story 3 (P3)**: Can start after Foundational (Phase 2) - Depends on US1 (partner entity) but independently testable
- **User Story 4 (P4)**: Can start after Foundational (Phase 2) - Depends on US3 (station entity) but independently testable

### Within Each User Story

- Tests (if included) MUST be written and FAIL before implementation
- Entities before repository implementations
- Repository implementations before services
- Services before handlers
- Handlers before route registration
- Story complete before moving to next priority

### Parallel Opportunities

- All Setup tasks marked [P] can run in parallel (T003-T007)
- All Foundational tasks marked [P] can run in parallel (T009-T016)
- Once Foundational phase completes, all user stories can start in parallel (if team capacity allows)
- Within each user story:
  - Test tasks marked [P] can run in parallel (e.g., T046-T052 for US2)
  - Entity creation tasks marked [P] can run in parallel (T019-T021 for US1)
  - Repository implementations marked [P] can run in parallel (T025-T027 for US1)
- Different user stories can be worked on in parallel by different team members

---

## Parallel Example: User Story 1

```bash
# Launch all entity creation tasks together:
Task: "Create Partner entity in platform-core/src/models/partner.rs"
Task: "Create Station entity in platform-core/src/models/station.rs"
Task: "Create Charger entity in platform-core/src/models/charger.rs"

# Launch all repository implementations together:
Task: "Implement PartnerRepository trait in domain layer"
Task: "Implement StationRepository trait in domain layer"
Task: "Implement ChargerRepository trait in domain layer"

# Launch all repository implementations in infrastructure layer together:
Task: "Implement PartnerRepositoryImpl in infrastructure layer"
Task: "Implement StationRepositoryImpl in infrastructure layer"
Task: "Implement ChargerRepositoryImpl in infrastructure layer"
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 1: Setup
2. Complete Phase 2: Foundational (CRITICAL - blocks all stories)
3. Complete Phase 3: User Story 1
4. **STOP and VALIDATE**: Test User Story 1 independently
5. Deploy/demo if ready

### Incremental Delivery

1. Complete Setup + Foundational → Foundation ready
2. Add User Story 1 → Test independently → Deploy/Demo (MVP!)
3. Add User Story 2 → Test independently → Deploy/Demo
4. Add User Story 3 → Test independently → Deploy/Demo
5. Add User Story 4 → Test independently → Deploy/Demo
6. Each story adds value without breaking previous stories

### Parallel Team Strategy

With multiple developers:

1. Team completes Setup + Foundational together
2. Once Foundational is done:
   - Developer A: User Story 1
   - Developer B: User Story 2
   - Developer C: User Story 3
   - Developer D: User Story 4
3. Stories complete and integrate independently

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story should be independently completable and testable
- Tests MUST fail before implementation (Red-Green-Refactor cycle)
- Tests cover: functional correctness, edge cases, error handling
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
- Run `cargo test` and `npm test` frequently
- Verify all tests pass before moving to next phase
- Follow TDD approach: write tests first, ensure they fail, then implement

---

## Task Statistics

**Total Tasks**: 103
**Parallel Tasks**: 48 (46.6%)
**User Story Tasks**: 71 (68.9%)
**Setup Tasks**: 7
**Foundational Tasks**: 9
**Polish Tasks**: 6

**Test Tasks**: 43 (all marked [P] or [US*] and independently testable)

**MVP Scope**: Phases 1-3 (T001-T045) - Setup + Foundational + User Story 1

---

**Last Updated**: 2026-06-23
**Version**: 1.0.0
**Status**: ✅ Ready for implementation
