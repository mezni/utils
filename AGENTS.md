<!-- SPECKIT START -->
# BorneMap — Agent Context

## Required Reading (Session Start)
Read these files at the start of every session:

1. `docs/governance/LLM_IMPLEMENTATION_GUIDE.md` — Master execution prompt
2. `.specify/memory/constitution.md` — BorneMap Constitution v1.15.2
3. `docs/speckit/sprints/<latest-sprint>/spec.md` — Current sprint spec

## Key Conventions
- All runtime code under `/source`
- All system definitions under `/docs`
- All infrastructure under `/infra`
- Rust services follow Clean Architecture (domain/application/infrastructure/presentation)
- Frontend follows ui-kit → domain-types → client-core dependency chain
- Entity IDs: PREFIX-nanoid(12) — users use Keycloak UUID
<!-- SPECKIT END -->
