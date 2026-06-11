# Network Zones (Zero Trust)

---

## Zone Model

```
┌──────────────────────────────┐
│         EDGE ZONE            │
│        Traefik Only          │
└──────────────┬───────────────┘
               │
┌──────────────▼───────────────┐
│     APPLICATION ZONE         │
│ Driver / Admin / Clickstream │
└──────────────┬───────────────┘
               │
┌──────────────▼───────────────┐
│        DATA ZONE             │
│ platform_db / analytics_db   │
└──────────────┬───────────────┘
               │
┌──────────────▼───────────────┐
│      IDENTITY ZONE           │
│        Keycloak              │
└──────────────────────────────┘
```

---

## Zone Rules

| Zone | Components | Access |
|---|---|---|
| Edge | Traefik | Internet inbound, routes to App zone |
| Application | driver-service, admin-service, clickstream-service | App zone only, connects to Data + Identity |
| Data | platform_db, analytics_db, keycloak_db | App zone services only, no external access |
| Identity | Keycloak | Internal only, accessed via Auth Gateway only |

---

## Routing Rules (Production MVP-6)

| Path | Target Service |
|---|---|
| `/api/v1/stations*` | Driver Service (8080) |
| `/api/v1/partners*` | Admin Service (8081) |
| `/api/v1/admin*` | Admin Service (8081) |
| `/api/v1/events*` | Clickstream Service (8082) |
| `/api/v1/auth*` | Driver Service / Auth Gateway |

---

## Security Guarantees

- No public database access
- No public Keycloak access
- No API gateway bypass
- Strict RBAC at service layer
- Dual-realm identity isolation
- Zero-trust internal network
