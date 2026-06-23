# E001 ARCHITECTURE

## Clean Architecture Model

presentation → application → domain → infrastructure

---

## Rules
- Actix in presentation only
- SQLx in infrastructure only
- Domain must be pure
- Application contains use-cases only
