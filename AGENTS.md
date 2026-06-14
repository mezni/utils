<!-- SPECKIT START -->
For additional context about technologies to be used, project structure,
shell commands, and other important information, read the current plan
at specs/002-backend-core-api/plan.md
<!-- SPECKIT END -->

🤖 AGENTS.md — BorneMap OpenCode Execution Brain

Version: 1.0
Status: Active
Role: System Execution Layer for OpenCode

1. 🧠 PURPOSE

This file defines how OpenCode must behave when working on BorneMap.

OpenCode is not an architect.

OpenCode is not a product designer.

OpenCode is an execution engine for pre-defined specifications only.

2. 🚫 ABSOLUTE FORBIDDEN BEHAVIOR

OpenCode MUST NEVER:

Create new services
Modify system architecture
Add endpoints outside spec
Extend MVP scope without instruction
Write code outside source/
Bypass @bm/api-client
Use fetch() or axios inside apps
Access users schema directly
Access keycloak_db
Introduce new libraries without approval
Implement features not in active MVP

3. 📦 ALLOWED WORKING AREAS

OpenCode may ONLY modify:

Frontend
source/front/apps/mobile-driver
source/front/apps/web-driver
source/front/apps/dashboard (MVP-2+ only)
Shared packages
source/front/packages/@bm/types
source/front/packages/@bm/api-client
source/front/packages/@bm/utils
source/front/packages/@bm/design-tokens
Backend (MVP scoped)
source/services/driver-service
source/services/admin-service (MVP-2+)
source/services/auth-service (MVP-3+)

4. 🧭 EXECUTION PRINCIPLE

OpenCode executes specifications. It does not generate them.

If a spec does not exist → STOP.

5. 📋 REQUIRED PRE-EXECUTION CHECKLIST

Before writing any code, OpenCode MUST confirm:

5.1 MVP Context
Which MVP is active?
What is the feature scope?
5.2 Feature Spec Exists
Is SpecKit document present?
Has UX/UI Pro Max defined behavior?
5.3 API Contract
Are endpoints defined in /api/v1/* spec?
5.4 Allowed Scope
Which folders are allowed for modification?
5.5 UX Constraints
Loading states defined?
Empty states defined?
Error states defined?

If ANY answer is missing → STOP.

6. 🧩 FEATURE EXECUTION MODEL

OpenCode MUST follow this order:

Step 1 — Read SpecKit

Understand:

inputs
outputs
constraints
Step 2 — Confirm API contract

Never invent endpoints

Step 3 — Identify file targets

Only modify allowed directories

Step 4 — Implement backend (if applicable)

Driver-service first (MVP-1)

Step 5 — Implement frontend

Using:

@bm/api-client
@bm/types
MapContainer abstraction
Step 6 — UX compliance

Ensure:

skeleton loading
empty states
error handling
mobile gestures (if applicable)
Step 7 — Validate scope

No extra features added

7. 🔌 API RULES
All endpoints MUST follow /api/v1/*
No unversioned routes allowed
No endpoint invention
No response shape modification outside spec

Allowed MVP-1 endpoints:

GET /api/v1/stations
GET /api/v1/stations/nearby
GET /api/v1/stations/{id}

8. 📱 FRONTEND RULES (CRITICAL)
Mandatory dependencies
@bm/api-client → ALL requests
@bm/types → ALL models
@bm/utils → ALL logic
@bm/design-tokens → ALL UI values
Forbidden in apps
fetch()
axios
direct map library usage
hardcoded colors or spacing
duplicated API logic
Map rule

All map rendering MUST go through:

MapContainer.ts
MapContainer.native.ts
MapContainer.web.ts

No exceptions.

9. 🧠 STATE RULES
Server state → React Query
UI state → local or Zustand per app
No shared global state across apps

10. 🗄️ DATA RULES

OpenCode MUST respect:

platform_db = system of record
analytics_db = append-only
gis = read-only
users = owned by auth-service only

11. 🔐 AUTH RULES
Only auth-service communicates with Keycloak
No frontend or backend bypass allowed
JWT is the only trusted identity mechanism for services

12. 🧪 TESTING RULES

OpenCode must add tests for:

API integration
critical UI flows (MVP-1 map flow)
utility functions

No feature is complete without basic test coverage.

13. 🚨 ERROR HANDLING RULES

Every feature MUST implement:

loading state (skeleton preferred)
empty state
error state with retry option

No silent failures allowed.

14. ⚙️ OUTPUT FORMAT (IMPORTANT)

When OpenCode completes a task, output MUST include:

1. Files modified
2. Reason for changes
3. API endpoints used
4. UI behavior changes
5. Any assumptions made

If assumptions exist → they must be explicitly stated.

15. 🧭 MVP ISOLATION RULE

Only ONE MVP is active at a time.

OpenCode MUST NOT:

implement future MVP features early
reference future services
prepare unused architecture

16. 🧠 CORE EXECUTION PRINCIPLE

OpenCode is a deterministic implementation engine driven by SpecKit and constrained by Constitution.

⚡ RESULT OF THIS FILE

This ensures:

zero architecture drift
no frontend/backend mixing
strict MVP execution
predictable LLM behavior
clean OpenCode output
