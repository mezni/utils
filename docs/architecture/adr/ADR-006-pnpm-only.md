# ADR-006: pnpm as Sole Package Manager (No npm, No yarn)

**Status:** Accepted  
**Date:** 2026-06-10  
**Authors:** Claude Code, Claude (chat)

---

## Context

BorneMap frontend projects (mobile-driver, web-driver, dashboard) require a package manager for dependencies:
- `react-native`, `expo`, `react-query`
- `zustand`, `react-native-reanimated`, `react-native-maps`
- Dev tools: `typescript`, `tailwind`, `eslint`

Three package managers are commonly used:
1. **npm** — default, bundled with Node.js
2. **yarn** (v1 or v3) — faster, deterministic
3. **pnpm** — space-efficient, fast, strict dependency isolation

The constitution mandates a single package manager (no mixing). Which one?

---

## Decision

**Use pnpm exclusively. Never use npm or yarn in BorneMap projects.**

Implementation:
1. **All frontend projects** (`mobile-driver`, `web-driver`, `dashboard`) use `pnpm`
2. **Lock file:** `pnpm-lock.yaml` (committed to version control)
3. **Node.js version:** 18 LTS or later (pnpm 8+ requirement)
4. **CI/CD:** Install via `corepack enable && pnpm install --frozen-lockfile`

---

## Rationale

### Why pnpm over npm?

| Aspect | npm | pnpm |
|--------|-----|------|
| **Speed** | Medium | Fast (parallel installs) |
| **Disk usage** | ~500MB per project | ~100MB (content-addressable store) |
| **Lockfile** | Huge, merge conflicts | Compact, minimal conflicts |
| **Determinism** | Good | Excellent (strict lockfile format) |
| **Peer deps** | Error-prone | Enforced correctly |
| **Monorepo** | Works, verbose | Optimized (workspaces) |

**Winner:** pnpm for speed, disk efficiency, and determinism.

### Why pnpm over yarn?

| Aspect | yarn | pnpm |
|--------|------|------|
| **Setup** | Requires global install | Via corepack (built-in) |
| **Performance** | Good | Better (parallel) |
| **Ecosystem** | Larger (older projects) | Growing, npm-compatible |
| **Breaking changes** | yarn v1 → v3 was major | Stable release cycle |
| **Team adoption** | Higher (legacy projects) | Lower (newer projects) |

**Context:** BorneMap is a **new project** with a **single implementation agent** (Claude Code). pnpm's efficiency and speed benefit CI/CD and dev cycle. Yarn's ecosystem advantage is moot for a new codebase.

**Winner:** pnpm for speed, built-in availability (corepack), and simplicity.

---

## Consequences

### Positive
- **Speed:** Install times reduced by 30-50%
- **Disk space:** Shared content-addressable store across projects
- **Lockfile quality:** Fewer merge conflicts, smaller diffs
- **Strictness:** Enforces correct peer dependency handling
- **Monorepo:** Optimized for multi-project workspaces (future)

### Negative
- **Familiarity:** Developers experienced with npm/yarn need brief ramp-up
- **Global install:** Requires corepack (Node 16.13+), can be skipped
- **Ecosystem:** Smaller community than npm (but npm-compatible)
- **Breaking changes:** Occasional updates require lockfile refresh

---

## Implementation Notes

1. **Installation (per developer):**
   ```bash
   # Enable corepack (one-time)
   corepack enable

   # Install dependencies
   pnpm install
   ```

2. **CI/CD setup:**
   ```bash
   corepack enable
   pnpm install --frozen-lockfile
   ```

3. **Common commands:**
   ```bash
   # Install dependency
   pnpm add lodash
   pnpm add -D typescript

   # Remove dependency
   pnpm remove lodash

   # Install all dependencies
   pnpm install

   # Run script
   pnpm run build

   # Update dependencies
   pnpm update
   ```

4. **Lockfile management:**
   - Commit `pnpm-lock.yaml` to version control
   - Never edit lockfile manually
   - For major updates: `pnpm update --interactive`

5. **Troubleshooting:**
   - If `ERR_PNPM_MINIMUM_RELEASE_AGE_VIOLATION` occurs:
     ```bash
     pnpm install --no-frozen-lockfile
     ```
   - If node_modules corruption suspected:
     ```bash
     pnpm store prune
     pnpm install
     ```

---

## Enforcement

1. **Pre-commit hook:** Block any `package-lock.json` or `yarn.lock` commits
2. **CI pipeline:** Fail if npm or yarn is detected (`which npm`, `which yarn`)
3. **Documentation:** Link to this ADR in setup guide

---

## Related ADRs

- ADR-003: Expo SDK 54 lock (pnpm enforces locked versions via frozen-lockfile)

---

## References

- [pnpm documentation](https://pnpm.io)
- [Node.js corepack](https://nodejs.org/api/corepack.html)
- [npm vs yarn vs pnpm comparison](https://www.npmtrends.com)
