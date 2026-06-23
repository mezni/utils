<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan:

**Feature**: EV Dashboard Platform Kernel
**Plan File**: specs/001-ev-dashboard/plan.md

**Related Documentation**:
- Specification: specs/001-ev-dashboard/spec.md
- Data Model: specs/001-ev-dashboard/data-model.md
- API Contracts: specs/001-ev-dashboard/contracts/
- Quickstart: specs/001-ev-dashboard/quickstart.md
- Research: specs/001-ev-dashboard/research.md

**Key Technologies**:
- Backend: Rust 1.75+ with Actix-Web and SQLx
- Frontend: React 18+ with TypeScript and TailwindCSS
- Database: PostgreSQL 16+ with 'ev' schema namespace
- Infrastructure: Docker and Docker Compose

**Architecture**: Clean Architecture (presentation → application → domain → infrastructure)

**Project Structure**:
- services/admin-service/ - Rust backend
- apps/admin-dashboard/ - React frontend
- crates/platform-core/ - Shared utilities
- crates/platform-db/ - Database abstraction

**API Version**: /api/v1

**External ID System**:
- Partners: PRT-<12-char>
- Stations: STA-<12-char>
- Chargers: CHR-<12-char>
<!-- SPECKIT END -->
