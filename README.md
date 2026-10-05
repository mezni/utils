# utils

A single repo for small, self-contained utilities. Each unit of work lives in its
own sub-directory with its own `pyproject.toml`, its own dependencies, and no
cross-imports — the repo is a container, not an application.

Nothing here is published to PyPI or npm. Projects are run from a checkout.

## Layout

| Directory      | Holds                                                       | Git |
|----------------|-------------------------------------------------------------|-----|
| `apps/`        | Runnable things with a UI or a server: CLI tools, web apps, services. | tracked |
| `skills/`      | Agent skills and prompt/instruction packs (`SKILL.md` + resources).     | tracked |
| `scripts/`     | One-off and operational shell/Python helpers run by hand or by cron.     | tracked |
| `generators/`  | Code that produces data or files: fixtures, corpora, scaffolds.         | tracked |
| `docs/`        | Cross-project notes and shared reference material.                     | tracked |

Inside a sub-directory, only the source is tracked. Dependencies, virtual
environments, build output, and generated data are ignored by the root
`.gitignore`, so a project directory stays clean in `git status`.

### Current contents

- `generators/docgen` — generates synthetic company/department policy documents
  (`pdf`, `html`, `docx`, `markdown`, `text`) through OpenRouter, tracking
  progress in `state.json` and writing files under `data/`.

## Conventions

- One project per sub-directory, named in `kebab-case`.
- A Python project gets its own `pyproject.toml` and is managed with `uv`. The
  repo root is a `uv init --bare` project, so it carries a `pyproject.toml` and
  `.venv` but no package layout.
- Dependencies stay local to the sub-project. Do not hoist them into the root
  `pyproject.toml`.
- Secrets come from the environment. Commit a `.env.example` with the keys you
  need; never commit `.env`.
- Generated output is not committed. If a project needs a committed fixture,
  put it somewhere other than the generator's own output directory.

## Working in a sub-project

```bash
cd generators/docgen
uv sync                 # create/refresh the local environment
uv run docgen.py --help
```

A project may also use its own toolchain — `npm install`, a `Makefile`, or a
plain script. Only the source needs to be committed.

### `generators/docgen`

Requires `OPENROUTER_API_KEY`; `DOCGEN_MODEL` overrides the model
(default `qwen/qwen3.8-27b:free`, chosen so it runs without credits).

```bash
cd generators/docgen
uv run docgen.py --industry "Renewable Energy"   # new company
uv run docgen.py --company-id calder              # append to an existing one
uv run docgen.py --list                           # show known companies
uv run docgen.py --fresh                          # discard state and start over
```

`--max-docs` caps documents per department (default 10). Run it from inside
`generators/docgen`, since paths are relative to the working directory.

## Adding a project

```bash
mkdir -p apps/my-tool && cd apps/my-tool
uv init --bare          # or your toolchain's equivalent
```

Commit the source and a `README.md`. Add a `.env.example` if it needs
configuration. Update the table above and `CHANGELOG.md`.

## Scaffolding a new repo

`scaffold.sh` resets this repo to a clean state: it deletes every file except
`.git`, `.gitignore`, `README.md`, `CHANGELOG.md`, and itself, then re-initialises
the `uv` project and pushes.

It is destructive by design and will remove `apps/`, `skills/`, `scripts/`, and
`generators/`. Do not run it here. It exists as a starting point for a fresh
repo of the same shape.

## Versioning

`CHANGELOG.md` follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
with [Semantic Versioning](https://semver.org/spec.php#pec-2.0.0). Each entry
records a version, a feature domain, and the objectives of the change.

## Prerequisites

- `uv` (0.12+)
- Python 3.12+ for Python projects
- Node.js for anything under `apps/` or `skills/` that needs it
- `OPENROUTER_API_KEY` for `generators/docgen`
