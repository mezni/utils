# Contributing

Thanks for your interest in contributing to the Telco SI reference BSS/OSS API.

## Code of Conduct

Be respectful and constructive. This is a reference implementation meant for learning and reuse.

## Getting Started

1. Fork the repository.
2. Set up the environment as described in [docs/OPERATIONS.md](docs/OPERATIONS.md).
3. Create a feature branch: `git checkout -b feature/your-feature`.

## Specification-Driven Workflow

This project is built feature-by-feature from specifications under `specs/` (e.g.
`specs/001-infra-multi-schema-engine/`), each containing a spec, contracts, plan,
and task list. Before writing code:

1. Read the relevant spec, contracts, and tasks.
2. Consult the process docs in `.specify/` and the `specs/*/plan.md`, `tasks.md`.
3. Mark tasks `[ ]` → `[X]` in `tasks.md` as you complete them, in the specified order.

## Development Workflow

1. **Understanding domain boundaries** — keep changes within the appropriate schema/context (`catalog`, `inventory`, `crm`, `usage`, `billing`, `dunning`). If you add a domain entity, do not introduce cross-schema foreign keys; rely on global identifiers (`UUID`, `MSISDN`, `ICCID`).
2. **Migrations** — add or modify an Alembic revision under `migrations/versions/` when schema-affecting logic changes:

   ```bash
   docker compose exec app alembic revision --autogenerate -m "description"
   docker compose exec app alembic upgrade head
   ```

   Applied migrations are immutable: the startup runner verifies each applied revision's file checksum against `public.alembic_revision_checksum`. Edit applied migrations only by adding a new revision, never by rewriting an applied file.
3. **Test** — add or update tests under `tests/` (`tests/contract/` for API contracts, `tests/integration/` for runtime behavior). Run the suite inside the container:

   ```bash
   docker compose exec app pytest -q
   ```

4. **Lint & format** — run locally and/or in the container:

   ```bash
   ruff check .
   ruff format .
   ruff format --check .
   ```

   Keep both local and in-container results clean.
5. **Document** — update the relevant file under `docs/` (see the README table of contents) whenever behavior changes, and keep `CHANGELOG.md` in sync.

## Commit Conventions

- Use clear, imperative commit messages.
- Scope the subject to the domain or feature, e.g. `infra(001): add revision checksum ledger`.
- Reference the feature (`specs/0xx-...`) your change belongs to.

## Pull Requests

- Verify the full flow: `docker compose up -d --build`, six schemas present, `alembic current` at head, `/health` returns `{"status":"ok","database":"up"}`, in-container `pytest -q` and `ruff check`/`ruff format --check` pass.
- Keep the docs tree and `CHANGELOG.md` in sync with code changes.
- Reference the spec/issue your PR addresses.

## Reporting Issues

Include:

- Reproduction steps.
- Expected vs actual behavior.
- Relevant logs or `docker compose` output.