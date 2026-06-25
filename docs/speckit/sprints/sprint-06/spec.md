# Sprint 06 — Admin Dashboard Bootstrap (UX/UI PRO MAX) + EV Domain Management

**Status**: SPEC WRITTEN
**Date**: 2026-06-25
**Constitution Version**: v1.15.2

---

## Scope Lock (HARD CONSTRAINT)

| Domain | Included | Excluded |
|--------|----------|----------|
| **Frontend** | `admin-dashboard` at `/source/apps/admin-dashboard/` | Any other apps |
| **Backend** | ❌ None | No backend changes, no new APIs, no DB changes |
| **Auth** | ❌ None | No authentication |
| **Analytics** | ❌ None | No analytics dashboard |
| **Inventory** | ❌ None | Only EV domain |

## Objective

Build a production-grade administrative dashboard for managing EV domain entities:
- Partners, Stations, Chargers
- Lookup tables (access types, data sources, connector types, etc.)

## Technology Stack

| Layer | Choice |
|-------|--------|
| Framework | React 18 + TypeScript |
| Build | Vite 6 |
| UI Components | shadcn/ui (built on Radix + Tailwind) |
| API Client | `@bornemap/client-core` (via TanStack Query) |
| Routing | React Router v7 |
| Styling | Tailwind CSS |
| Map | Leaflet (future-ready) |

## UX/UI PRO MAX Requirements

### Core Principle
Professional SaaS administration platform feel.

### UX States
Every screen MUST implement: Loading | Empty | Success | Error — no blank screens.

### Accessibility
Keyboard navigation, screen reader support, focus management, accessible form validation.

## Application Layout

```
┌───────────────────────────────────┐
│ Header                            │
├────────────┬──────────────────────┤
│ Sidebar    │ Content Area         │
│ Navigation │                      │
└────────────┴──────────────────────┘
```

## Routes

| Route | Component | Description |
|-------|-----------|-------------|
| `/dashboard` | DashboardPage | Summary cards |
| `/data/partners` | PartnersPage | Partner CRUD |
| `/data/stations` | StationsPage | Station CRUD |
| `/data/chargers` | ChargersPage | Charger CRUD |
| `/settings` | SettingsPage | Lookup tables management |

## Sidebar Menu

- **Dashboard** → `/dashboard`
- **Data** (parent with submenus):
  - Partners → `/data/partners`
  - Stations → `/data/stations`
  - Chargers → `/data/chargers`
- **Settings** → `/settings`

## Modules

### Dashboard
Summary cards showing: Partners Count, Stations Count, Chargers Count

### Partners (`/data/partners`)
List, search, create, edit, soft-delete partners via admin-service API

### Stations (`/data/stations`)
List, search, filter by partner, create, edit, soft-delete stations

### Chargers (`/data/chargers`)
List, filter by station, create, edit, soft-delete chargers

### Settings (`/settings`)
Manage lookup tables: Access Types, Data Sources, Connector Types, Current Types, Connector Statuses

## Architecture Compliance

| Constitution Rule | Check | Status |
|------------------|-------|--------|
| §7.3 Dependency chain | ui-kit → domain-types → client-core | ✅ |
| §7.3 No business logic in UI | Use cases in client-core only | ✅ |
| §10.3 Identity separation | PREFIX-nanoid(12) displayed only | ✅ |
| UX/UI PRO MAX | All states required | ✅ |
