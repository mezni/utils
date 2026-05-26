# Research Report: Admin Portal — Shell, Navigation & BaseMap

## Technical Decisions

### Map Library: Leaflet + react-leaflet v4

- **Decision**: Use Leaflet with react-leaflet v4 binding
- **Rationale**: Constitution mandates Leaflet in approved tech stack. react-leaflet v4 provides native React component wrappers (MapContainer, TileLayer, Marker, Popup). CartoDB light tiles are supported out of the box. Lightest-weight tile map option for React.
- **Alternatives considered**: Google Maps (requires API key, heavier bundle), MapLibre (more complex setup, overkill for 100 markers)

### Routing: React Router v6 with nested routes

- **Decision**: React Router v6 with `<Outlet/>` pattern for section routing
- **Rationale**: Spec assumptions document this as existing project convention. Nested routes map cleanly to sidebar sections. URL-based deep linking works naturally.
- **Alternatives considered**: TanStack Router (newer but not yet in project), reach-router (merged into React Router)

### State Management: React hooks + context

- **Decision**: useState/useContext for sandbox toggle, no Redux or external state library
- **Rationale**: Admin portal has minimal shared state (sandbox toggle only). React built-in hooks are sufficient. Redux would add unnecessary complexity.
- **Alternatives considered**: Zustand (lightweight but unnecessary), Redux (too heavy for this scope)

### Design Tokens: Tailwind config with theme extension

- **Decision**: Extend `packages/ui/tailwind.config.cjs` with custom theme values (colors, borderRadius, spacing, boxShadow)
- **Rationale**: Per docs/03-web-admin-ux-spec.md — all tokens centralized in one file. Hardcoded hex codes banned in view files. Extending Tailwind's theme ensures type-safe access via className strings.
- **Alternatives considered**: CSS custom properties alone (no Tailwind integration), styled-components (deviates from existing Tailwind convention)

### Loading States: Skeleton placeholders

- **Decision**: Skeleton shapes matching metric chip and map dimensions while data loads
- **Rationale**: Per spec clarification Q2. Skeletons give immediate layout feedback vs blank spinners. Standard UX pattern for data dashboards.
- **Alternatives considered**: Spinner (provides no layout context), nothing (blank flash on load)

### Marker Interaction: Popup with info + "View Details" link

- **Decision**: Click marker → popup shows name, city, charger count + "View Details" link → click link → navigate to station detail page
- **Rationale**: Per spec clarification Q1. Popup preserves map context. User can see multiple stations and decide which to explore further.
- **Alternatives considered**: Direct navigation (loses map context), tooltip only (no detail navigation)

### Sandbox Persistence: localStorage

- **Decision**: Sandbox toggle state persisted in localStorage under a well-known key
- **Rationale**: Per spec clarification Q3. localStorage survives page reloads and browser restarts. No backend round-trip needed.
- **Alternatives considered**: sessionStorage (resets on tab close), in-memory (resets on page reload)

## Key Integration Points

| Integration | Details |
|-------------|---------|
| Backend API | `/api/v1/stations` — GET list of stations (for map markers and metric count) |
| Backend API | `/api/v1/partners` — GET partners count |
| Backend API | `/api/v1/chargers` — GET chargers count (or derive from station data) |
| Tile server | `https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png` (CartoDB light) |
| Design system | `packages/ui/tailwind.config.cjs` — single source of truth for tokens |
| Auth scaffold | Phase 1 JWT auth — admin must be authenticated before accessing portal |

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Map tile server accessibility | Graceful fallback if tiles fail to load (empty map canvas, no crash) — per FR-012 |
| API unavailability on load | Skeleton placeholders shown initially; error state replaces skeleton on failure |
| Large station count on map (>200) | Phase 3 scope limited to ~100 seed stations. If needed later, add marker clustering |
| Cross-browser Tailwind compatibility | Vite + PostCSS + Tailwind v3 handles vendor prefixes automatically |
