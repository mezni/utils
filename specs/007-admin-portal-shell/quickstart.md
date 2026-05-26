# Quickstart: Admin Portal — Shell, Navigation & BaseMap

## Prerequisites

- Node.js 20+
- pnpm 9+
- Backend API running (Phase 1 + Phase 2)
- Docker Compose stack (for backend services)

## Setup

```bash
cd sources/frontend
pnpm install
```

## Development

```bash
# Start admin portal dev server
cd apps/admin-portal
pnpm dev
```

The admin portal is served at `http://localhost:5173` (Vite default).

## Validation Checklist

### 1. AppShell Layout
- [ ] Open `http://localhost:5173` (or configured port)
- [ ] Verify persistent layout: sidebar on the left, main content area on the right
- [ ] Resize browser to 1024px width — layout remains functional (sidebar visible)

### 2. Sidebar Navigation
- [ ] All six navigation items visible: Overview, Users, Data, Analytics, Security, Settings
- [ ] Click "Overview" — Overview section loads, Overview highlighted in sidebar, URL updates to `/`
- [ ] Click "Users" — Users section loads, Users highlighted, URL updates to `/users`
- [ ] Click "Data" — Data section loads, Data highlighted, URL updates to `/data`
- [ ] Click "Analytics" — Analytics section loads, Analytics highlighted, URL updates to `/analytics`
- [ ] Click "Security" — Security section loads, Security highlighted, URL updates to `/security`
- [ ] Click "Settings" — Settings section loads, Settings highlighted, URL updates to `/settings`
- [ ] Navigate directly to `http://localhost:5173/users` — correct section loads, correct nav item highlighted

### 3. Overview Dashboard — Metric Chips
- [ ] Three metric chips visible: "Total Stations", "Total Chargers", "Total Partners"
- [ ] Chips show non-negative integer values (matching backend data)
- [ ] During loading, skeleton placeholders are shown instead of empty chips
- [ ] If backend is unreachable, chips show error state (not crash)

### 4. Overview Dashboard — BaseMap
- [ ] Interactive map visible, centered on Tunisia
- [ ] Map tiles load from CartoDB (light theme)
- [ ] Station markers visible as green circles with lightning bolt icons
- [ ] Click a station marker — popup appears with station name, city, available charger count
- [ ] Popup contains a "View Details" link
- [ ] If backend is unreachable, map shows empty state (not crash)

### 5. Sandbox Workspace Toggle
- [ ] Sandbox toggle visible in the header
- [ ] Click toggle — `border-t-4 border-sky-500` blue border appears at top of layout
- [ ] Click toggle again — blue border disappears
- [ ] Refresh the page — sandbox state persists from previous session
- [ ] Rapid toggle (click multiple times quickly) — no glitch or double-border

### 6. Design System Components
- [ ] Open the Settings section — `<SettingsCard/>` renders with rounded-2xl corners, card shadow, proper padding
- [ ] `<SelectSetting/>` dropdown has rounded-md styling (visible if any dropdown exists)
- [ ] Data tables (if present) scroll horizontally on narrow viewports
- [ ] Destructive action (delete) triggers `<ConfirmDeleteModal/>` — button disabled until exact resource ID is typed

### 7. Placeholder Pages
- [ ] Users section shows a functional placeholder (not a blank white page)
- [ ] Data section shows a functional placeholder (not a blank white page)
- [ ] Analytics section shows a functional placeholder (not a blank white page)
- [ ] Security section shows a functional placeholder (not a blank white page)
- [ ] Settings section shows a functional placeholder (not a blank white page)
- [ ] Overview Dashboard shows placeholder cards for analytics features (post-MVP0)

### 8. Error Boundaries
- [ ] Simulate a rendering error in any section — only that section breaks, the rest of the portal remains navigable
- [ ] Console has no unhandled errors during normal navigation flow

## Test Commands

```bash
# Type-check
cd sources/frontend && pnpm -r type-check

# Lint
cd sources/frontend && pnpm -r lint

# Run unit tests
cd sources/frontend && pnpm -r test

# Build
cd sources/frontend && pnpm -r build
```
