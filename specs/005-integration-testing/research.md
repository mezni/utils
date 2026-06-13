# Research: Integration & Testing

**Phase 0 output** — Technical decisions for `specs/005-integration-testing/spec.md`

## R-001: Mobile E2E Test Framework

**Decision**: Maestro

**Rationale**: Maestro is a mobile E2E test framework that works with both iOS and Android without requiring app modification or SDK integration. It supports gestures, assertions, and flows via simple YAML-based test files. For Expo-based apps, Maestro can test against the Expo Go or development build APK/IPA without detox-specific native module configuration. Maestro's flow-based approach aligns well with the Given/When/Then acceptance scenarios defined in the spec.

**Alternatives considered**:
- Detox: Requires native module integration and specific build configuration. More complex setup for Expo managed workflow. Better for React Native apps that already have Detox configured, but Phase 4 didn't set it up.
- Appium: Overkill for this scope. Selenium-based, slower, more infrastructure needed.
- Manual testing: Not automated — fails the CI integration requirement (FR-012).

## R-002: Contract Testing Tool

**Decision**: Pact (pact-js / pact-rust)

**Rationale**: Pact is the industry standard for consumer-driven contract testing. It supports both TypeScript (for mobile/web app consumers) and Rust (for service providers). Pact tests can be integrated into the existing GitHub Actions CI pipeline. Pact generates contract files that can be shared between consumer and provider teams, ensuring API compatibility.

**Alternatives considered**:
- Postman/Newman collections: Not automated contract testing — requires manual comparison.
- OpenAPI schema validation (swagger/express-openapi-validate): Validates at runtime, not contract testing in the Pact sense. Useful as a complement but doesn't provide consumer-driven contracts.
- Hand-rolled assertion scripts: No ecosystem, no contract sharing mechanism.

**Scope note**: Phase 5 implements provider-side Pact tests only (server validates contracts). Consumer-side Pact tests for mobile/web apps are deferred — mobile apps are version-locked to API contracts and catching contract breaks server-side is sufficient for MVP-1.

## R-003: Traefik Routing Configuration

**Decision**: Static file-based configuration in `infra/traefik/dynamic.yml`

**Rationale**: For local development, static file-based Traefik configuration is simpler and more transparent than Docker provider labels or consul-based dynamic configuration. The routing rules are well-known and stable (driver-service :8080, admin-service :8081). No need for dynamic routing discovery in MVP-1.

**Routes**:
- `PathPrefix /api/v1/stations` → `http://driver-service:8080`
- `PathPrefix /api/v1/admin` → `http://admin-service:8081`
- `PathPrefix /api/v1/events` → `http://admin-service:8081`
- `Path `/health` → `http://driver-service:8080`

**Middleware**:
- Rate limiting: 100 req/s per IP (basic protection)
- Error handling: Return 503 when upstream unavailable
- Forwarded headers: Pass X-Forwarded-* for service logging

## R-004: Load Testing Tool

**Decision**: k6

**Rationale**: k6 is a modern, scriptable load testing tool with native JavaScript/TypeScript support. It integrates well with CI pipelines, supports concurrent virtual users (matching our 50 concurrent request target), and provides p50/p95/p99 latency reporting out of the box. k6 can be run via Docker without installation.

**Alternatives considered**:
- autocannon: Node.js-only, simpler but less feature-rich for concurrent user simulation.
- wrk/ bombardier: Go-based, fast but no scripting — cannot simulate the multi-step nearby search flow.
- ab (ApacheBench): Too basic — no latency percentiles, no scriptable flows.

## R-005: CI Pipeline Integration Strategy

**Decision**: Multi-stage pipeline in GitHub Actions

**Rationale**: The constitution mandates GitHub Actions (from Phase 3 CI workflow). Integration tests need a different workflow than unit tests because they require a running environment (Docker Compose with Traefik + databases + services).

**Pipeline stages**:
1. **Build services**: Compile driver-service and admin-service binaries (caching for speed)
2. **Docker Compose up**: Start Traefik, PostGIS, admin-service, driver-service with seed data
3. **Contract tests**: Run Pact contract tests against running services (fast, <2 min)
4. **E2E tests**: Run Maestro mobile E2E and web E2E tests (requires emulator — may need matrix)
5. **Load tests**: Run k6 tests against Traefik endpoint (target: <100ms p95 at 50 concurrent)
6. **Test report**: Aggregate all results into a single report artifact

**Note**: Mobile E2E tests on CI may require Android emulator or hardware device. If not available in CI, mobile E2E tests can be flagged as manual with web-only E2E in CI.
