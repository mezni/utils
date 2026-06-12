# ADR-003: Expo SDK 54 Lock

**Status:** Accepted  
**Date:** 2026-06-10  
**Authors:** Claude Code, Claude (chat)

---

## Context

The mobile driver app uses **Expo SDK**, which updates frequently (every 6 months). Each major version introduces:
- Breaking changes to dependencies (e.g., react-native-maps, reanimated versions)
- Native layer updates (Cocoa, Gradle)
- Increased app size
- Potential regressions in map rendering or performance

BorneMap's mobile experience is a primary differentiator. **Map stability and UX performance are non-negotiable.**

Without a version lock, we risk:
1. Accidental SDK upgrades breaking production
2. Dependency cascades (one package forces SDK upgrade)
3. Binary bloat (newer SDK = larger app download)
4. Performance regressions during critical features

---

## Decision

**Lock Expo SDK at version 54 indefinitely.**

Upgrades require:
1. **Explicit ADR** — must be filed before any upgrade
2. **Testing sweep** — all map interactions, animations, dark mode tested on real devices
3. **Stabilization sprint** — two weeks minimum of testing
4. **Approval** — constitution change required

This applies to:
- `expo` package version
- `expo-router` (locked at v3)
- `react-native-maps` (version compatible with SDK 54)
- `react-native-reanimated` (v3, compatible with SDK 54)

---

## Rationale

### Stability First
BorneMap's map experience is the product. Expo SDK upgrades are forced, unplanned regressions. Locking prevents them.

### Dependency Clarity
When all deps are locked to known-good versions, future issues have fewer variables.

### Predictable Deployments
No surprise breaking changes during routine refactors or feature work.

### UX Continuity
Mobile drivers rely on consistent map behavior. Locking ensures it.

---

## Consequences

### Positive
- **Stability:** No surprise SDK regressions
- **Predictability:** Dependencies remain constant across sessions
- **Performance:** No unplanned app size increases
- **Developer experience:** Fewer compatibility issues

### Negative
- **Security patches:** Must evaluate and backport if critical
- **New features:** Can't use latest Expo features without ADR + sprint
- **Maintenance debt:** Eventually must upgrade (e.g., when support ends)
- **Team friction:** Dev cycle feels slower compared to "always upgrade"

### Upgrade Path (Future)
When SDK 54 reaches end-of-support (estimated 2028):
1. File ADR-XXX-expo-sdk-upgrade
2. Allocate full stabilization sprint
3. Test all map + animation flows on real devices
4. Update constitution if new constraints discovered

---

## Implementation Notes

1. **Lock in Expo config (`app.json`):**
   ```json
   {
     "expo": {
       "sdkVersion": "54.0.0"
     }
   }
   ```

2. **Lock in `package.json`:**
   ```json
   {
     "expo": "54.0.0",
     "expo-router": "3.4.0",
     "react-native-maps": "1.10.0",
     "react-native-reanimated": "3.5.0"
   }
   ```

3. **Lock strategy:**
   - Use exact versions (no `^` or `~` ranges)
   - Use `pnpm install --frozen-lockfile` in CI/CD
   - Document any version exceptions with rationale

4. **Tooling:**
   - Use Dependabot with "manual review required" for any updates
   - Monitor Expo security announcements monthly

---

## Related ADRs

- ADR-006: pnpm only (enforces consistent lock files)
- ADR-001: Traefik (no backend version lock needed — can evolve freely)

---

## References

- [Expo SDK release schedule](https://docs.expo.dev/eas-update/getting-started/)
- [Expo SDK 54 changelog](https://docs.expo.dev/versions/v54.0.0/)
- [Semantic versioning](https://semver.org)
