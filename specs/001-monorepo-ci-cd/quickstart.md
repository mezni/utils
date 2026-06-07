# Quickstart: Monorepo and CI/CD Setup

## Prerequisites

- Rust 1.95+ (`rustup install 1.95`)
- Node.js 20.20+ (`nvm install 20.20`)
- npm 10.8+ (included with Node.js)
- Docker 24+ with Docker Compose

## Setup

```bash
git clone git@github.com:mezni/BorneMap.git
cd BorneMap

# Install Rust dependencies
cargo build --all

# Install JS/TS dependencies
npm install
```

## Verify

```bash
# Rust workspace compiles
cargo build --all

# Shared crate tests pass
cargo test -p ev-core
cargo test -p ev-db

# Frontend dependencies installed
npm ls --depth=0

# Docker Compose starts
docker compose -f infra/compose/docker-compose.yml up -d
docker compose -f infra/compose/docker-compose.yml ps
```

## CI Workflows

Six workflows in `.github/workflows/`:

| Workflow | Trigger | What it checks |
|---|---|---|
| `ci.yml` | Any push | Full workspace: Rust lint+test, frontend lint+build |
| `ci-driver-service.yml` | Driver Service changes | Rust integration tests with PostgreSQL |
| `ci-admin-service.yml` | Admin Service changes | Rust integration tests with PostgreSQL |
| `ci-driver-web.yml` | Driver Web changes | ESLint, TypeScript, Vite build |
| `ci-driver-mobile.yml` | Driver Mobile changes | ESLint, TypeScript check |
| `ci-dashboard.yml` | Dashboard changes | ESLint, TypeScript, Vite build |
