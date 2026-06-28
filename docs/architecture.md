# BorneMap Architecture

## System Overview

```
┌─────────────┐    ┌─────────────┐
│ Admin       │    │ Driver      │
│ Dashboard   │    │ Web         │
│ :9001       │    │ :9002       │
└──────┬──────┘    └──────┬──────┘
       │                  │
       │   HTTP/JSON      │
       ▼                  ▼
┌──────────────┐  ┌──────────────┐
│ admin-service│  │driver-service│
│ :3002        │  │ :3003        │
├──────────────┤  ├──────────────┤
│ ev + gis     │  │ ev + gis     │
│ (auth req)   │  │ (public)     │
└──────┬───────┘  └──────┬───────┘
       │                 │
       ▼                 ▼
┌──────────────────────────────┐
│    PostgreSQL 15 + PostGIS   │
│  ┌──────┐ ┌────┐ ┌──────┐   │
│  │users │ │ ev │ │ gis  │   │
│  └──────┘ └────┘ └──────┘   │
└──────────────────────────────┘
       ▲
┌──────┴───────┐
│ auth-service │
│ :3001        │
│ (users)      │
└──────────────┘
```

## Clean Architecture (per service)

```
src/
├── main.rs                 # Entry point, server bootstrap
├── config/
│   └── mod.rs              # Route registration
├── presentation/
│   └── http/               # HTTP handlers, DTOs
├── application/            # Use cases / orchestration
├── domain/                 # Domain entities, business rules
└── infrastructure/         # DB repositories, external clients
```

**Rules:**
- `domain/` has zero external dependencies
- `presentation/` depends on `application/` and `domain/`
- `application/` depends on `domain/` and `infrastructure/` (via traits)
- `infrastructure/` depends on `domain/` (implements repository traits)
