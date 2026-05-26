# Research: Admin Data Views & CRUD

## Technology Decisions

### Decision: Modal Forms for Create/Edit
- **Chosen**: Modal dialogs overlaid on the data table page
- **Rationale**: Keeps user context (the table) visible; simpler than dedicated form routes; aligns with existing `<ConfirmDeleteModal/>` modal pattern. Modal contains form with validation, submission, and success/error feedback.
- **Alternatives considered**: Dedicated pages (adds route complexity), inline editing (not supported by `<ScrollableTable/>`)

### Decision: Dual Charger Views (Flat + Nested)
- **Chosen**: `/data/chargers` flat list with station filter + nested `/stations/:id/chargers` view
- **Rationale**: Flat view enables cross-cutting operations (find all faulted chargers); nested view provides context within station management. Station filter on flat view implemented as a `<SelectSetting/>` dropdown.
- **Alternatives considered**: Nested-only (no cross-station visibility), flat-only (loses station context)

### Decision: Bidirectional Map-Table Interaction
- **Chosen**: Table row click → `map.flyTo(station.coordinates)`; marker click → scroll table row into view + highlight with background color
- **Rationale**: `flyTo` provides smooth animated pan; row scroll + highlight provides clear visual feedback. Implementation uses react-leaflet's `useMap()` hook for imperative map control and a ref-based table row scroll.
- **Alternatives considered**: Hard navigation (jumps without animation), side-panel detail (adds layout complexity)

### Decision: Data Refetch After Mutation
- **Chosen**: Re-fetch the list after create/edit/delete instead of optimistic UI update
- **Rationale**: Simplest correct approach for MVP0; ensures consistency with server state. Optimistic updates can be added post-MVP0 if latency becomes an issue.
- **Alternatives considered**: Optimistic updates (complex rollback logic), manual refresh (confusing UX)

### Decision: Status Badge Colors
- **Chosen**: CSS classes via Tailwind design tokens (bg-green-500, bg-amber-500, bg-red-500, bg-gray-500) — no hardcoded hex
- **Rationale**: Follows Principle III (no hardcoded hex); tokens are defined in tailwind.config.cjs
- **Alternatives considered**: Inline styles (violates constitution), icon-only (less scannable)

### Decision: Empty States
- **Chosen**: Centered message with icon within the table area: "No [entities] found"
- **Rationale**: Clear, friendly, consistent across all entity types. Icon from inline SVG (same approach as sidebar nav).
- **Alternatives considered**: Hide table entirely (user may wonder if page is broken), show 0-row table (confusing)

### Decision: No Inline Search/Filter (MVP0)
- **Chosen**: Table displays all records; no client-side search or column filtering for MVP0
- **Rationale**: Seed data is ~100-300 records; scroll+scan is sufficient. Search is a post-MVP0 enhancement.
- **Alternatives considered**: Client-side filter inputs (adds complexity without demonstrated need), server-side search API (backend endpoint not specified)

### Decision: Connector Type Dropdown Refresh
- **Chosen**: Refetch connector types from API when Chargers create/edit modal opens
- **Rationale**: Simple and reliable; ensures the dropdown always reflects current state. The 5-second SC-003 tolerance allows for a brief loading state.
- **Alternatives considered**: WebSocket push (over-engineered for MVP0), polling (wasteful), localStorage cache with invalidation flag (more complex)
