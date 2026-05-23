# Contributing to BorneMap

Welcome, and thank you for considering contributing to BorneMap.

## Before you start

Please read the following documents — they govern every contribution:

- [Constitution](.specify/memory/constitution.md) — principles, rules, and governance
- [Roadmap](docs/roadmap.md) — project phases and milestones

## Repository governance

The repository recognises two contributor roles:

- **Maintainer**: has merge rights on `main`, approves ADR and Constitution amendments. Current Maintainers: [@mezni/bornemap-maintainers](https://github.com/orgs/mezni/teams/bornemap-maintainers).
- **Contributor**: anyone who opens a PR or files an issue. All contributors are expected to follow this guide.

ADR approval and Constitution amendments require Maintainer sign-off.

## How to propose a change

1. Pick a card from the project board linked to a roadmap phase.
2. Create a branch following the branch strategy below.
3. Make your changes, keeping commits atomic and signed off.
4. Open a pull request — the template will guide you through the required sections.
5. Address reviewer feedback and ensure all checks pass.
6. A Maintainer merges your PR once the Definition of Done is satisfied.

## Branch strategy

Trunk-based development on `main`.

- **Base branch**: `main`
- **Feature branches**: `phase-<N>/<short-slug>` (e.g., `phase-5/outbox-relay-worker`)
- **ADR branches**: `adr/<number>-<short-slug>` (e.g., `adr/0006-new-service`)
- **Hotfix branches**: `hotfix/<short-slug>` (e.g., `hotfix/critical-bug`)
- **Merge style**: squash merge with Conventional Commits (`feat:`, `fix:`, `docs:`, `chore:`, `refactor:`, `test:`, `perf:`, `ci:`)
- **Lifecycle**: branches auto-delete on merge. Branches with no commits for 30 days and no open PR are flagged for Maintainer review.

## Commit messages

- Follow [Conventional Commits](https://www.conventionalcommits.org/).
- Every commit **must** include a `Signed-off-by:` trailer matching the commit author to certify the contributor's right to submit the work under the [Developer Certificate of Origin](https://developercertificate.org/).

```bash
git commit -s -m "feat: add station search endpoint"
```

The `-s` flag appends the `Signed-off-by:` trailer automatically.

## Pull-request workflow

When you open a PR, the [pull request template](.github/PULL_REQUEST_TEMPLATE.md) will guide you through:

- Declaring the roadmap phase and Constitution principles your change relates to.
- Listing the changes.
- Checking the applicable test categories from Principle VII.
- Completing the Definition of Done checklist.

Every PR must be reviewed and approved per `CODEOWNERS` before merge.

## Filing or amending an ADR

If your change affects a constitutional boundary (service responsibilities, data ownership, identity provider, event pipeline, soft-delete scope, deployment topology, approved stack), you **must** file an ADR first.

1. Copy [docs/adr/template.md](docs/adr/template.md) to `docs/adr/NNNN-short-slug.md`.
2. Fill in Status, Context, Decision, Consequences.
3. Open a PR on branch `adr/NNNN-short-slug`.
4. If the ADR amends the Constitution, bump `.specify/memory/constitution.md` in the same PR.

## Reporting issues

Open an issue in the repository. Include a clear description, steps to reproduce (if applicable), and the roadmap phase your issue relates to.
