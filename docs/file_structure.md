# BorneMap — Canonical File Structure
**Version:** 1.0
**Date:** June 2026
**Status:** Frozen for validation phase

---

This document is the sole authority for allowed file paths. No file outside this structure may be created without a constitutional amendment.

```
bornemap/
├── constitution/
│   ├── constitution.md
│   ├── guardrails.md
│   └── standards.md
│
├── docs/
│   ├── architecture/
│   │   └── architecture.md
│   ├── governance/
│   ├── adr/
│   ├── templates/
│   ├── SYSTEM_STATE.md
│   ├── roadmap_status.md
│   └── file_structure.md
│
├── source/
│   ├── services/
│   │   ├── auth-service/
│   │   │   ├── api/
│   │   │   ├── domain/
│   │   │   ├── application/
│   │   │   ├── infrastructure/
│   │   │   └── Cargo.toml
│   │   ├── driver-service/
│   │   │   ├── api/
│   │   │   ├── domain/
│   │   │   ├── application/
│   │   │   ├── infrastructure/
│   │   │   └── Cargo.toml
│   │   └── admin-service/
│   │       ├── api/
│   │       ├── domain/
│   │       ├── application/
│   │       ├── infrastructure/
│   │       └── Cargo.toml
│   │
│   ├── apps/
│   │   ├── web/
│   │   ├── dashboard/
│   │   └── mobile/
│   │
│   ├── shared/           # Rust crates only
│   │   ├── auth-core/
│   │   ├── db-models/
│   │   ├── geo/
│   │   ├── validation/
│   │   ├── error/
│   │   └── utils/
│   │
│   └── packages/         # TypeScript packages only
│       ├── shared-types/
│       ├── shared-ui/
│       ├── shared-hooks/
│       ├── api-client/
│       ├── auth-client/
│       ├── config/
│       └── utils/
│
├── platform/
│   ├── infrastructure/
│   │   ├── docker/
│   │   ├── traefik/
│   │   ├── keycloak/
│   │   ├── postgres/
│   │   └── redis/
│   │
│   ├── api/
│   │   └── openapi/
│   │       ├── identity.yaml
│   │       ├── driver.yaml
│   │       ├── admin.yaml
│   │       └── shared.yaml
│   │
│   ├── scripts/
│   │
│   └── tools/
│       ├── sprint_engine.sh
│       ├── validate.sh
│       ├── reconcile.sh
│       ├── github_sync.sh
│       ├── test_runner.sh
│       └── ci_guard.sh
│
├── execution/
│   ├── state/
│   │   ├── sprint_state.json
│   │   ├── mapping.json
│   │   ├── phase_registry.json
│   │   └── transition_log.json
│   │
│   ├── backlog/
│   │   ├── epics/
│   │   ├── features/
│   │   └── roadmap/
│   │
│   ├── intelligence/
│   │   ├── auto_sprint_reports/
│   │   ├── dependency_graphs/
│   │   └── speckit_validations/
│   │
│   ├── reports/
│   │   ├── audits/
│   │   ├── security/
│   │   ├── coverage/
│   │   └── performance/
│   │
│   └── sprints/
│       └── sprint-NNN/
│           ├── spec/
│           ├── backlog/
│           ├── design/
│           ├── api/
│           ├── implementation/
│           ├── testing/
│           ├── bugs/
│           │   ├── active.md
│           │   ├── resolved.md
│           │   └── regression_log.md
│           ├── review/
│           ├── state/
│           └── artifacts/
│               ├── generated_files_index.md
│               └── checksum_manifest.json
│
├── logs/
│
├── .github/
│   ├── workflows/
│   │   ├── ci.yml
│   │   ├── sprint-sync.yml
│   │   ├── contract-guard.yml
│   │   └── dependency-graph.yml
│   ├── ISSUE_TEMPLATE/
│   └── PULL_REQUEST_TEMPLATE.md
│
├── Cargo.toml
├── pnpm-workspace.yaml
├── package.json
└── README.md
```
