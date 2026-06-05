# Clean Architecture

Every backend service MUST follow Clean Architecture with four layers:

## Layer Structure

```
┌─────────────────────────────────────┐
│        Interface Layer (HTTP)        │
│   handlers, middleware, router       │
├─────────────────────────────────────┤
│     Application Layer (Use Cases)    │
│   service logic, orchestration      │
├─────────────────────────────────────┤
│    Infrastructure Layer (External)   │
│   database access, external APIs    │
├─────────────────────────────────────┤
│         Domain Layer (Pure)          │
│   entities, value objects, rules    │
└─────────────────────────────────────┘
```

## Rules

1. **Domain layer** has zero dependencies on external frameworks
2. **Application layer** depends only on domain layer
3. **Infrastructure layer** implements interfaces defined by application
4. **Interface layer** handles HTTP concerns only
5. Dependencies point inward — outer layers depend on inner layers
