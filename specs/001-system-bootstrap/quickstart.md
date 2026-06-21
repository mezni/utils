# Quickstart: System Bootstrap & Enforcement Kernel

**Feature**: 001-system-bootstrap
**Date**: 2026-06-21
**Branch**: `001-system-bootstrap`

## Prerequisites

- **Rust 1.75+** — Required for cargo, sqlx, and Rust toolchain
- **PostgreSQL 14+** — Required for database schemas
- **Docker & Docker Compose** (optional) — For local development environment
- **Git** — For version control and CI pipeline
- **GitHub Actions** — For CI/CD execution (automated)

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/your-org/bornemap.git
cd bornemap
git checkout 001-system-bootstrap
```

### 2. Install Rust Toolchain

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
rustc --version  # Should be 1.75 or higher
cargo --version
```

### 3. Install PostgreSQL

**Linux (Ubuntu/Debian)**:
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

**macOS**:
```bash
brew install postgresql@14
brew services start postgresql@14
```

### 4. (Optional) Install Docker & Docker Compose

**Linux**:
```bash
sudo apt install docker.io docker-compose
sudo usermod -aG docker $USER
newgrp docker
```

**macOS**:
```bash
brew install --cask docker
open /Applications/Docker.app
```

**Windows**:
- Install Docker Desktop from https://www.docker.com/products/docker-desktop

## Project Structure

```
bornemap/
├── apps/packages/          # Frontend packages
├── services/               # Backend microservices
├── tools/                  # CI enforcement scripts
├── infrastructure/         # DevOps configuration
├── docs/                   # Documentation
├── .specify/               # SpecKit configuration
└── specs/                  # Feature specifications
```

## Development Setup

### 1. Create Virtual Environment

```bash
# Create .cargo/config.toml for workspace settings
mkdir -p .cargo
cat > .cargo/config.toml << EOF
[workspace]
members = [
    "apps/packages/ui-kit",
    "apps/packages/domain-types",
    "apps/packages/client-core",
    "services/auth-service",
    "services/driver-service",
    "services/admin-service",
]

[profile.release]
opt-level = 3
lto = true
codegen-units = 1
EOF
```

### 2. Verify Workspace Structure

```bash
cargo check --workspace
```

### 3. Run Tests

```bash
cargo test --workspace
```

### 4. Format Code

```bash
cargo fmt --all
```

### 5. Lint Code

```bash
cargo clippy --all-targets --all-features --workspace
```

## Database Setup

### Manual Setup (No Docker)

```bash
# Create databases
createdb platform_db
createdb analytics_db

# Run migrations (each service has its own migrations)
cd services/auth-service
cargo run --bin migrate

cd ../driver-service
cargo run --bin migrate

cd ../admin-service
cargo run --bin migrate
```

### Docker Compose Setup (Recommended)

```bash
cd infrastructure/docker-compose
docker-compose -f local.yml up -d

# Verify services are running
docker-compose -f local.yml ps

# Access PostgreSQL
docker exec -it bornemap-postgres psql -U bornemap_user -d platform_db
docker exec -it bornemap-postgres psql -U bornemap_user -d analytics_db
```

### Initialize Databases

```bash
# Create schemas (manual approach)
psql -U postgres -d platform_db -f services/auth-service/migrations/0001_init_users.up.sql
psql -U postgres -d platform_db -f services/driver-service/migrations/0001_init_gis.up.sql
psql -U postgres -d platform_db -f services/admin-service/migrations/0001_init_inventory.up.sql
psql -U postgres -d analytics_db -f services/driver-service/migrations/0002_init_analytics.up.sql
psql -U postgres -d analytics_db -f services/driver-service/migrations/0003_create_analytics_indexes.up.sql
```

## Service Skeletons

### Start Services (Manual)

```bash
# Terminal 1: auth-service
cd services/auth-service
cargo run --bin auth-service
# Service runs on port 3000

# Terminal 2: driver-service
cd services/driver-service
cargo run --bin driver-service
# Service runs on port 3001

# Terminal 3: admin-service
cd services/admin-service
cargo run --bin admin-service
# Service runs on port 3002
```

### Start Services (Docker Compose)

```bash
cd infrastructure/docker-compose
docker-compose -f local.yml up -d auth-service
docker-compose -f local.yml up -d driver-service
docker-compose -f local.yml up -d admin-service

# Check logs
docker-compose -f local.yml logs -f auth-service
docker-compose -f local.yml logs -f driver-service
docker-compose -f local.yml logs -f admin-service
```

### Health Check Services

```bash
curl http://localhost:3000/health
curl http://localhost:3001/health
curl http://localhost:3002/health
```

Expected response:
```json
{
  "status": "ok",
  "timestamp": "2026-06-21T12:00:00Z",
  "service": "<service-name>"
}
```

## CI Pipeline

### Run CI Locally

```bash
# Format check
cargo fmt --all

# Type check
cargo clippy --all-targets --all-features --workspace

# Dependency validation
cargo tree

# Schema validation
bash tools/04_validate_schema.sh

# SQLx compile check
cargo sqlx prepare --check

# Run all CI validations
bash tools/ci_guard.sh
```

### Run Full CI Pipeline (GitHub Actions)

```bash
# Push to trigger CI
git add .
git commit -m "[Sprint 0] Complete system bootstrap"
git push origin 001-system-bootstrap
```

The CI pipeline will execute all 9 stages:
1. format_check
2. type_check
3. dependency_graph_validation
4. identity_validation
5. schema_validation
6. sqlx_compile_check
7. analytics_write_gate
8. integration_tests
9. build_success

## Validation

### Verify Identity System

```bash
# Run identity validation
bash tools/01_validate_identity.sh

# Should pass if no UUID in entity identifiers
# Should fail if nanoid used in users
```

### Verify Dependencies

```bash
# Run dependency validation
bash tools/02_validate_deps.sh

# Should pass if no forbidden edges exist
# Should fail if service→service imports detected
```

### Verify Analytics Gate

```bash
# Run analytics gate validation
bash tools/03_validate_analytics_gate.sh

# Should pass if only driver-service has write access
# Should fail if admin-service has write access
```

### Verify Database Schema

```bash
# Run schema validation
bash tools/04_validate_schema.sh

# Should pass if all tables and indexes exist
# Should fail if tables are missing
```

### Verify SQLx Policy

```bash
# Run SQLx policy check
bash tools/05_sqlx_policy_check.sh

# Should pass if no raw SQL strings exist
# Should fail if dynamic SQL detected
```

## Troubleshooting

### Database Connection Errors

**Problem**: Cannot connect to PostgreSQL

**Solution**:
```bash
# Check PostgreSQL is running
sudo systemctl status postgresql

# Check database exists
psql -U postgres -l | grep platform_db
psql -U postgres -l | grep analytics_db

# Restart PostgreSQL if needed
sudo systemctl restart postgresql
```

### Service Won't Start

**Problem**: Service fails to start on port 3000, 3001, or 3002

**Solution**:
```bash
# Check if port is already in use
lsof -i :3000
lsof -i :3001
lsof -i :3002

# Kill process on port if needed
kill -9 $(lsof -t -i:3000)

# Rebuild and restart service
cd services/auth-service
cargo clean
cargo build --release
cargo run --bin auth-service
```

### SQLx Preparation Error

**Problem**: `sqlx prepare` fails with "prepare output file missing"

**Solution**:
```bash
# Run sqlx prepare to generate offline data
cd services/auth-service
cargo sqlx prepare -- --database-url postgres://user:password@localhost:5432/platform_db

# Do the same for other services
cd services/driver-service
cargo sqlx prepare -- --database-url postgres://user:password@localhost:5432/analytics_db

cd services/admin-service
cargo sqlx prepare -- --database-url postgres://user:password@localhost:5432/platform_db
```

### CI Pipeline Fails

**Problem**: CI pipeline fails at a specific stage

**Solution**:
1. Check the error message for the failing stage
2. Fix the issue locally (formatting, type errors, etc.)
3. Re-run the failing stage manually to verify fix
4. Commit and push

Common failure reasons:
- **format_check**: Run `cargo fmt --all`
- **type_check**: Run `cargo clippy --all-targets`
- **dependency_graph_validation**: Check for forbidden edges
- **identity_validation**: Fix UUID/nanoid mixing
- **schema_validation**: Fix missing tables/indexes
- **sqlx_compile_check**: Run `cargo sqlx prepare --check`
- **analytics_write_gate**: Fix write permission violations
- **integration_tests**: Run `cargo test --workspace`
- **build_success**: Run `cargo build --workspace`

## Development Workflow

1. **Create Feature Branch**:
   ```bash
   git checkout main
   git pull origin main
   git checkout -b 001-add-user-authentication
   ```

2. **Implement Feature**:
   - Add code to the appropriate package/service
   - Run `cargo fmt --all`
   - Run `cargo clippy --all-targets`
   - Run `cargo test --workspace`

3. **Validate Locally**:
   ```bash
   bash tools/01_validate_identity.sh
   bash tools/02_validate_deps.sh
   bash tools/04_validate_schema.sh
   ```

4. **Create Pull Request**:
   - Push changes to remote
   - Open PR on GitHub
   - CI pipeline will automatically run

5. **Review and Merge**:
   - CI must pass all 9 stages
   - Review code changes
   - Merge to main

## Useful Commands

### Workspace Commands

```bash
# Check all crates
cargo check --workspace

# Run all tests
cargo test --workspace

# Build all crates
cargo build --workspace

# Clean all crates
cargo clean
```

### Service-Specific Commands

```bash
# Check auth-service
cd services/auth-service
cargo check
cargo test
cargo clippy

# Check driver-service
cd services/driver-service
cargo check
cargo test
cargo clippy

# Check admin-service
cd services/admin-service
cargo check
cargo test
cargo clippy
```

### Database Commands

```bash
# List databases
psql -U postgres -l

# Connect to database
psql -U postgres -d platform_db

# Run migration
psql -U postgres -d platform_db -f migrations/0001_init_users.up.sql

# Check tables
\dt

# Check schema
\d users

# Exit
\q
```

## Next Steps

After completing Sprint 0:

1. **Review System State**:
   - Check `docs/SYSTEM_STATE.md`
   - Review `docs/roadmap_status.md`
   - Read `docs/sprints/sprint_00/review/sprint_00_review.md`

2. **Proceed to Next Sprint**:
   - Sprint 1: Identity & Security Core
   - Sprint 2: GIS Engine Foundation
   - Continue through sprints 3-8

3. **Verify Compliance**:
   - Check constitution compliance
   - Validate identity system
   - Verify data ownership
   - Run CI pipeline

## Resources

- **Constitution**: [docs/constitution/constitution.md](../docs/constitution/constitution.md)
- **Architecture**: [docs/architecture.md](../architecture.md)
- **Sprint 0 Backlog**: [docs/sprints/sprint_00/backlog/backlog.md](../sprints/sprint_00/backlog/backlog.md)
- **Data Model**: [data-model.md](./data-model.md)
- **Contracts**: [contracts/](./contracts/)

## Support

For issues or questions:
1. Check [Troubleshooting](#troubleshooting) section
2. Review constitution at [docs/constitution/constitution.md](../docs/constitution/constitution.md)
3. Check CI pipeline logs on GitHub Actions
4. Contact team lead or architect