# Research: Dev Environment + CI/CD + Runnable Skeleton

## Decisions

### Backend Framework: Actix Web
- **Decision**: Actix Web with Tokio async runtime
- **Rationale**: Mandated by project constitution (Principle II). Provides
  mature Rust web ecosystem, high-performance async request handling, and
  strong middleware composition.
- **Alternatives considered**: Axum, Rocket — rejected by constitution
  (framework replacement prohibited during validation unless blocker proven)

### Health Endpoint Pattern
- **Decision**: Separate `/health/live` and `/health/ready` endpoints under
  `/api/v1` scope
- **Rationale**: Industry-standard Kubernetes health check pattern. Liveness
  = service is running. Readiness = service can accept traffic (no external
  deps to verify in Phase 1, so both return OK).
- **Alternatives considered**: Single combined endpoint — rejected because
  readiness suggests dependency checking readiness for future phases

### Monorepo Tooling
- **Decision**: pnpm workspaces + turbo for JS/TS; Cargo workspace for Rust
- **Rationale**: pnpm is standard for React Native/Expo monorepos; Cargo
  workspace is the idiomatic Rust multi-crate approach. Turbo provides
  caching and parallel task execution for CI.
- **Alternatives considered**: npm workspaces, yarn workspaces —
  constitution/spec specified pnpm

### Logging: Structured JSON
- **Decision**: Structured JSON logging with timestamp, level, message,
  service fields via env_logger
- **Rationale**: Clarified during spec review. JSON is machine-parseable
  and works with log aggregation tooling.
- **Alternatives considered**: Plain text — rejected for operational
  readiness

### Mobile Backend Connectivity
- **Decision**: Direct fetch from Expo Go to `http://localhost:8080/api/v1/health/live`
- **Rationale**: Simple HTTP fetch for Phase 1. Native HTTP client in
  React Native handles this without additional libraries.
- **Alternatives considered**: Axios, Apollo Client — unnecessary for Phase 1

### Prerequisite Validation
- **Decision**: Integrated startup check — backend validates prerequisites
  inline and reports clear error messages
- **Rationale**: Clarified during spec review. Single command workflow
  without manual pre-flight scripts.
- **Alternatives considered**: Separate check script — rejected for UX
