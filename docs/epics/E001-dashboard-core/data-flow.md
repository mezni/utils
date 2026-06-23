# E001 DATA FLOW

User Action
→ React Component
→ React Query
→ API Client
→ admin-service
→ application layer
→ domain layer
→ infrastructure layer (persistence, mappers)
→ database
→ response returns upward

---

## Delete Operations Flow

Hard Delete:
User Action → DELETE endpoint → application layer → domain layer → infrastructure layer → database (CASCADE) → response

Soft Delete:
User Action → PUT endpoint → application layer → domain layer → infrastructure layer → database (no cascade) → response

Undelete:
User Action → PUT endpoint → application layer → domain layer → infrastructure layer → database (no cascade) → response
