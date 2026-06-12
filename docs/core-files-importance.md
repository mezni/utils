# Core Files and Their Importance

This document captures the critical files that must exist to prevent the LLM from making inconsistent decisions. Each file serves as a "rulebook" that guides code generation.

---

## 1. Database Schema Files

### `infra/migrations/001_platform_db_init.sql`

**Purpose:** Defines the inventory schema with stations + chargers tables using PostGIS geometry, entity-prefixed IDs, soft delete, and indexes.

**Why Critical:**
- Without this DDL, the LLM will invent column names like `station_name`, `charger_status`, etc.
- Real schema prevents invalid SQL queries
- Enforces entity-prefixed IDs (`STA-`, `CHR-`, `PRT-`)
- Guarantees PostGIS geometry usage
- Enforces soft delete pattern

**Required Schema:**
```sql
CREATE TABLE inventory.partner (
    id VARCHAR(50) PRIMARY KEY,  -- PRT-{nanoid}
    name VARCHAR(255) NOT NULL UNIQUE,
    contact_email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE inventory.station (
    id VARCHAR(50) PRIMARY KEY,  -- STA-{nanoid}
    name VARCHAR(255) NOT NULL,
    address VARCHAR(255) NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lng DOUBLE PRECISION NOT NULL,
    location GEOMETRY(Point, 4326) GENERATED ALWAYS AS
        (ST_SetSRID(ST_Point(lng, lat), 4326)) STORED,
    status VARCHAR(20) NOT NULL DEFAULT 'offline',
    opening_hours VARCHAR(255),
    partner_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP,
    FOREIGN KEY (partner_id) REFERENCES inventory.partner(id)
);

CREATE TABLE inventory.charger (
    id VARCHAR(50) PRIMARY KEY,  -- CHR-{nanoid}
    station_id VARCHAR(50) NOT NULL,
    type VARCHAR(20) NOT NULL,  -- CCS2|CHAdeMO|Type2|GBT|Type1
    power_kw FLOAT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'offline',
    price_per_kwh FLOAT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES inventory.station(id)
);
```

**Enforces:**
- PostGIS geometry with SRID 4326
- Soft delete (`deleted_at` field)
- Spatial indexes for performance
- Entity-prefixed nanoid IDs

---

### `infra/migrations/003_analytics_db_init.sql`

**Purpose:** Defines the raw_events table with append-only constraints.

**Why Critical:**
- Without this, the LLM will invent event schemas
- Enforces append-only pattern (no UPDATE, no DELETE)
- Prevents data modification errors
- Ensures event audit trail

**Required Schema:**
```sql
CREATE TABLE analytics.raw_events (
    id VARCHAR(50) PRIMARY KEY,  -- EVT-{nanoid}
    event_type VARCHAR(50) NOT NULL,  -- station_viewed|charger_selected|booking_started|...
    user_id VARCHAR(50),
    metadata JSONB NOT NULL,
    occurred_at TIMESTAMP NOT NULL DEFAULT NOW(),
    session_id VARCHAR(50),
    device_id VARCHAR(50)
);

-- Append-only constraint (no modifications to existing rows)
CREATE TRIGGER enforce_append_only
BEFORE UPDATE ON analytics.raw_events
FOR EACH ROW EXECUTE FUNCTION disable_update();

CREATE TRIGGER enforce_append_only_delete
BEFORE DELETE ON analytics.raw_events
FOR EACH ROW EXECUTE FUNCTION disable_delete();
```

**Enforces:**
- Append-only pattern (immutable events)
- Event categorization (event_type)
- User and session tracking
- Device and timestamp metadata

---

### `infra/migrations/004_seed_stations.sql`

**Purpose:** Tunisia seed data with real coordinates.

**Why Critical:**
- Without real coordinates, the map screen cannot be tested at all
- Provides baseline data for QA testing
- Ensures map functionality is testable
- Prevents "no stations found" false negatives

**Required Data:**
```sql
INSERT INTO inventory.partner (id, name, contact_email) VALUES
('PRT-tunisie1', 'Tunisie Énergie', 'contact@tunisieenergie.tn'),
('PRT-elle1', 'Elle Campus', 'support@ellecampus.tn');

INSERT INTO inventory.station (id, name, address, lat, lng, partner_id) VALUES
('STA-001', 'Station Tunis Centre', 'Avenue Habib Bourguiba, Tunis', 36.8065, 10.1815, 'PRT-tunisie1'),
('STA-002', 'Station Carrefour Ettadhamen', 'Route de la Soukra, Ettadhamen', 36.8670, 10.1630, 'PRT-tunisie1'),
('STA-003', 'Station Djerba Mall', 'Route de Djerba, Houmt Souk', 33.8378, 10.9320, 'PRT-elle1');

INSERT INTO inventory.charger (id, station_id, type, power_kw, price_per_kwh) VALUES
('CHR-001', 'STA-001', 'CCS2', 150.0, 0.45),
('CHR-002', 'STA-001', 'Type2', 22.0, 0.50),
('CHR-003', 'STA-002', 'CHAdeMO', 50.0, 0.48);
```

**Enforces:**
- Real geographic coordinates for testing
- Baseline test data for QA
- Multiple charger types for testing
- Partner-station relationships

---

## 2. Infrastructure Files

### `infra/docker-compose.yml`

**Purpose:** Local infrastructure contract defining service configuration, port mappings, and dependencies.

**Why Critical:**
- Without it, the LLM invents port mappings, volume names, env var names
- Prevents port conflicts and incorrect service discovery
- Ensures consistent database connectivity
- Guarantees all services can talk to each other

**Required Configuration:**
```yaml
version: '3.9'

services:
  driver-service:
    build: ./source/driver-service
    ports:
      - "8080:8080"
    environment:
      - DATABASE_URL=postgresql://admin:password@postgres:5432/platform_db
      - RUST_LOG=info
    depends_on:
      - postgres
    networks:
      - bornemap

  admin-service:
    build: ./source/admin-service
    ports:
      - "8081:8081"
    environment:
      - DATABASE_URL=postgresql://admin:password@postgres:5432/platform_db
      - RUST_LOG=info
    depends_on:
      - postgres
    networks:
      - bornemap

  postgres:
    image: postgis/postgis:16-3.3
    environment:
      - POSTGRES_DB=platform_db
      - POSTGRES_USER=admin
      - POSTGRES_PASSWORD=password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - bornemap

networks:
  bornemap:
    driver: bridge

volumes:
  postgres_data:
```

**Enforces:**
- Service ports (8080, 8081)
- Database connectivity
- Network isolation
- Volume persistence

---

### `infra/.env.example`

**Purpose:** Documents every required environment variable for Docker Compose and service configurations.

**Why Critical:**
- The LLM won't know what to inject without this registry
- Prevents runtime errors from missing env vars
- Ensures consistent configuration across environments
- Documents all service dependencies

**Required Variables:**
```bash
# Database
DATABASE_URL=postgresql://admin:password@localhost:5432/platform_db
DATABASE_SSL_MODE=disable

# Services
DRIVER_SERVICE_PORT=8080
ADMIN_SERVICE_PORT=8081

# Logging
RUST_LOG=info
LOG_LEVEL=info

# JWT (Keycloak)
JWT_SECRET=your-secret-key
JWT_ISSUER=bm-drivers

# CORS
CORS_ALLOWED_ORIGINS=http://localhost:8080
```

**Enforces:**
- Complete environment variable registry
- Default values and descriptions
- Required vs optional classification

---

## 3. Design System Files

### `source/mobile-driver/design/tokens.ts`

**Purpose:** Single enforcement point for Rule 11 (no hardcoded design values).

**Why Critical:**
- The LLM will hardcode values the moment this file doesn't exist
- Ensures consistent design language across components
- Provides dark/light theme variants
- Centralizes all design decisions

**Required Content:**
```typescript
// tokens.ts
export const tokens = {
  colors: {
    light: {
      background: '#ffffff',
      surface: '#f3f4f6',
      primary: '#3b82f6',
      text: '#000000',
      border: '#e5e7eb',
      error: '#ef4444',
      success: '#10b981'
    },
    dark: {
      background: '#000000',
      surface: '#1a1a1a',
      primary: '#60a5fa',
      text: '#ffffff',
      border: '#262626',
      error: '#f87171',
      success: '#34d399'
    }
  },
  spacing: {
    xs: 4,
    sm: 8,
    md: 16,
    lg: 24,
    xl: 32,
    '2xl': 48
  },
  typography: {
    fontSize: {
      xs: 12,
      sm: 14,
      md: 16,
      lg: 18,
      xl: 20,
      '2xl': 24,
      '3xl': 32
    },
    fontWeight: {
      normal: '400',
      medium: '500',
      semibold: '600',
      bold: '700'
    }
  },
  borderRadius: {
    sm: 8,
    md: 12,
    lg: 16,
    xl: 24
  },
  shadows: {
    sm: { shadowColor: '#000', shadowOffset: { width: 0, height: 1 }, shadowOpacity: 0.1, shadowRadius: 2 },
    md: { shadowColor: '#000', shadowOffset: { width: 0, height: 2 }, shadowOpacity: 0.15, shadowRadius: 4 },
    lg: { shadowColor: '#000', shadowOffset: { width: 0, height: 4 }, shadowOpacity: 0.2, shadowRadius: 8 }
  }
};
```

**Enforces:**
- No hardcoded colors, spacing, or typography
- Dark/light theme variants
- Consistent design system
- Rule 11 compliance

---

### `source/mobile-driver/design/theme.ts`

**Purpose:** Dark/light theme object that consumes tokens.

**Why Critical:**
- Without it, every component author reimplements theming differently
- Ensures theme switching works globally
- Centralizes theme logic
- Prevents theme inconsistencies

**Required Content:**
```typescript
// theme.ts
import { tokens } from './tokens';

export const lightTheme = {
  colors: tokens.colors.light,
  typography: tokens.typography,
  spacing: tokens.spacing,
  borderRadius: tokens.borderRadius,
  shadows: tokens.shadows
};

export const darkTheme = {
  colors: tokens.colors.dark,
  typography: tokens.typography,
  spacing: tokens.spacing,
  borderRadius: tokens.borderRadius,
  shadows: tokens.shadows
};

export type Theme = typeof lightTheme | typeof darkTheme;

export const useTheme = () => {
  const [theme, setTheme] = useState<Theme>(lightTheme);

  return {
    theme,
    toggleTheme: () => setTheme(theme === lightTheme ? darkTheme : lightTheme)
  };
};
```

**Enforces:**
- Theme switching mechanism
- Global theme context
- Consistent theme application

---

## 4. Documentation Files

### `docs/architecture/adr/` — 4 ADRs

**Purpose:** Documents major architectural decisions.

**Why Critical:**
- Without ADRs, the LLM will second-guess these decisions mid-session
- Prevents contradictory decisions
- Provides rationale for long-term consistency
- Serves as decision history

**Required ADRs:**
1. **ADR-001:** Traefik as gateway
2. **ADR-003:** Expo SDK 54 lock
3. **ADR-004:** Clickstream in admin-service
4. **ADR-007:** Source-rooted codebase

**Content Structure:**
```markdown
# ADR-001: Traefik as API Gateway

**Date:** 2026-06-10
**Status:** Accepted
**Decision:** All client traffic routes through Traefik.

## Context
...

## Decision
...

## Consequences
...

## Alternatives Considered
...

## Implementation
...

## Testing
...
```

**Enforces:**
- Consistent architectural decisions
- Rationale for long-term consistency
- Decision history and rollback capability

---

### `docs/database/platform-db-schema.md`

**Purpose:** Human-readable schema reference.

**Why Critical:**
- The LLM needs to know column names, types, and relationships before writing queries
- Provides query examples
- Documents indexes and constraints
- Serves as system of record for schema evolution

**Required Content:**
- Table definitions with descriptions
- Column metadata (type, nullable, notes)
- Indexes and constraints
- Query examples by use case
- Migration strategy

**Enforces:**
- Accurate schema understanding
- Query generation correctness
- Index utilization awareness

---

### `docs/mvp/mvp-1-discovery-core.md`

**Purpose:** Scoped task list as a document — not just the visual board.

**Why Critical:**
- Claude Code reads files, not widgets
- Provides detailed breakdown by phase
- Includes work breakdown structure
- Tracks status and deliverables

**Required Content:**
- **Scope** — What MVP-1 delivers
- **Architecture Decisions** — ADR references
- **Work Breakdown** — Phases, tasks, owners
- **Deliverables** — What is produced
- **Definition of Done** — Launch criteria
- **Known Risks & Mitigations**
- **Success Metrics**
- **Success Criteria Checklist**

**Enforces:**
- Clear scope boundaries
- Detailed task breakdown
- Success criteria definition
- Phased implementation

---

### `docs/skills/uiux-pro-max/SKILL.md`

**Purpose:** Enforcement mechanism for Section 7 of the constitution.

**Why Critical:**
- Still the biggest gap
- Without this, Section 7 of the constitution has no teeth
- Defines exact implementation standards
- Provides testing checklist

**Required Content:**
- Core principles (skeleton screens, optimistic UI, haptics)
- Design token discipline
- Component guidelines
- Quality standards
- Implementation rules
- Testing checklist

**Enforces:**
- Constitutional rule compliance
- UX quality standards
- Implementation best practices
- Testing requirements

---

## 5. MVP-Specific Files

### Migration Order Requirements

**Recommended Generation Order:**
1. `infra/migrations/001_platform_db_init.sql` ← stations + chargers DDL
2. `infra/migrations/003_analytics_db_init.sql` ← raw_events DDL
3. `infra/docker-compose.yml` ← local infra contract
4. `infra/.env.example` ← env var registry
5. `source/mobile-driver/design/tokens.ts` ← design system foundation
6. `source/mobile-driver/design/theme.ts` ← dark/light theme
7. `infra/migrations/004_seed_stations.sql` ← Tunisia seed data
8. `docs/mvp/mvp-1-discovery-core.md` ← scoped task list
9. `docs/architecture/adr/` (×4) ← decision records

---

## Why These Files Matter

### Preventing LLM Inconsistency

1. **Database Schema** → LLM invents columns without DDL
2. **Docker Compose** → LLM invents ports, volumes, env vars
3. **Design Tokens** → LLM hardcodes values without source of truth
4. **Seed Data** → LLM can't test map without real coordinates
5. **ADRs** → LLM second-guesses architectural decisions
6. **Human-Readable Schema** → LLM generates incorrect queries
7. **MVP Task List** → LLM invents tasks without scope boundaries
8. **Theme File** → LLM reimplements theming differently per component
9. **Env Example** → LLM misses required environment variables
10. **UX Pro Max Skill** → LLM violates constitutional rules

### Ensuring Quality

These files collectively ensure:
- **Correctness:** Valid database schemas, correct queries
- **Consistency:** Uniform design system, consistent themes
- **Testability:** Real data for QA, reproducible environments
- **Maintainability:** Clear decision history, documented patterns
- **Quality:** UX standards enforced, constitutional compliance

---

## Summary

**These 10 files form the foundation of the BorneMap project.** Without them, the LLM will make incorrect, inconsistent, or incomplete decisions. With them, the LLM has a clear rulebook to follow, ensuring high-quality, consistent, and testable code.

**Priority Levels:**
- **Critical (Must Have):** 1-7 (Database, Infra, Design, MVP docs)
- **Important (Should Have):** 8-9 (Theme, Env)
- **Nice to Have (Enhances Quality):** 10 (UX Pro Max)

**Enforcement:**
- These files must exist before any code generation
- These files must be updated when decisions change
- These files serve as the LLM's source of truth
