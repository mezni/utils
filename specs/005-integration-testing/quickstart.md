# Quickstart: Integration & Testing

## Prerequisites

- Docker Compose installed
- Node.js 20+ and pnpm installed
- Expo CLI installed (`npm install -g expo-cli`)
- Android emulator or iOS simulator (for mobile E2E)

## Setup

### 1. Start Infrastructure

```bash
cd infra
docker-compose up -d
```

This starts: PostgreSQL 16 + PostGIS, driver-service, admin-service, Traefik gateway (all on `localhost:8080`).

### 2. Verify Traefik Routing

```bash
# Health check through gateway
curl http://localhost:8080/health

# Stations endpoint
curl http://localhost:8080/api/v1/stations

# Admin endpoint
curl http://localhost:8080/api/v1/admin/stations
```

Expected: Each returns a valid JSON response from the correct service.

### 3. Run Contract Tests

```bash
cd source/services/driver-service
cargo test --test contract_tests

cd source/services/admin-service
cargo test --test contract_tests
```

### 4. Run E2E Tests (Mobile)

```bash
cd source/front/mobile-driver

# Start Expo
npx expo start

# In another terminal, run Maestro E2E tests
maestro test e2e/discovery-flow.yaml
```

### 5. Run Load Tests

```bash
cd specs/005-integration-testing/tests
k6 run load-test.js
```

Expected: P95 nearby search latency < 100ms at 50 concurrent requests.

## Test Scenarios Quick Reference

| Test | Command | Location |
|------|---------|----------|
| Contract tests (driver) | `cargo test --test contract_tests` | `source/services/driver-service` |
| Contract tests (admin) | `cargo test --test contract_tests` | `source/services/admin-service` |
| Mobile E2E (discovery) | `maestro test e2e/discovery-flow.yaml` | `source/front/mobile-driver` |
| Mobile E2E (dark mode) | `maestro test e2e/dark-mode.yaml` | `source/front/mobile-driver` |
| Web E2E | `npx playwright test e2e/` | `source/front/web-driver` |
| Traefik routing | `bash tests/traefik-routing.sh` | `specs/005-integration-testing/tests` |
| Event logging | `bash tests/event-logging.sh` | `specs/005-integration-testing/tests` |
| Auth rejection | `bash tests/auth-rejection.sh` | `specs/005-integration-testing/tests` |
| Load test | `k6 run tests/load-test.js` | `specs/005-integration-testing/tests` |
| Test report | `bash tests/report.sh` | `specs/005-integration-testing/tests` |

## CI Pipeline

Tests are run automatically via GitHub Actions on every push — see `.github/workflows/integration-tests.yml`. Pipeline stages:

1. **Contract tests**: Driver-service + admin-service Pact tests (parallel)
2. **Web E2E**: Playwright tests against Chrome
3. **Load tests**: k6 performance benchmarks (50 concurrent VUs)
4. **Routing tests**: Traefik routing + auth rejection via Docker Compose
