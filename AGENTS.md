<!-- SPECKIT START -->
For additional context about technologies, project structure, shell commands,
and other important information, read the current implementation plan:
`specs/003-mobile-driver-app/plan.md`

## Frontend Development Rules

All frontend apps must follow the rules in `source/docs/constitution.md`:
- **§2** — Code sharing boundary (shared hooks/types vs platform-specific map views)
- **§5a** — State-driven interface checklist (loading/success/empty/error), map interaction constraints (300ms debounce, zoom thresholds), mobile-specific rules (Expo Go only, AsyncStorage offline fallback), web-specific rules (Leaflet asset optimization, Tailwind consistency), security (token isolation, zero input mutation)
<!-- SPECKIT END -->
