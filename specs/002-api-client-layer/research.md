# Research: API Client Layer

## Resolved Decisions

### Decision: HTTP Transport Strategy

**Decision**: Use platform-native `fetch` via a thin transport abstraction

**Rationale**: Both browser (web) and React Native (mobile) provide a standard `fetch` API. Wrapping it in a lightweight transport class allows future customization (auth headers, base URL injection, logging interceptors) without coupling to a third-party HTTP library. Avoids adding `axios` or `ky` as a dependency.

**Alternatives considered**:
- `axios` — adds 14KB+ bundle overhead, unnecessary for 3 simple GET calls
- `ky` — adds dependency, native `fetch` sufficient
- Direct `fetch` in each function — violates DRY; transport abstraction preferred

### Decision: Error Handling Pattern

**Decision**: Create a typed `ApiError` class with `status`, `message`, and optional `data` fields. Client methods throw `ApiError` on non-2xx responses and network failures.

**Rationale**: Consistent error shape across all three functions enables callers to handle errors uniformly. Extends `Error` so it works naturally with `try/catch` and `instanceof` checks.

**Alternatives considered**:
- Plain objects — lose stack traces, less ergonomic
- Error codes enum — overkill for 3 endpoints; add later if needed

### Decision: Testing Approach

**Decision**: Vitest + msw (Mock Service Worker) for HTTP mocking

**Rationale**: Vitest is the monorepo standard. msw intercepts `fetch` at the network level, allowing full integration-style tests without a running backend.

**Alternatives considered**:
- Jest — not aligned with monorepo toolchain
- Manual `fetch` mock — fragile, harder to maintain

### Decision: Package Export Strategy

**Decision**: Single default export — a factory function `createApiClient(baseUrl)` returning an object with the three typed methods

**Rationale**: Simple, tree-shakeable, single configuration point. Aligns with the spec requirement (FR-009).

**Alternatives considered**:
- Class with constructor — more ceremony, same result
- Individual named exports — loses base URL configuration context
