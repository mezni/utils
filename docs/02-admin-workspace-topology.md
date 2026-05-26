# BorneMap — Administrative Workspace Topology

## Overview

The Admin Portal frontend layout explicitly partitions platform capabilities into dedicated structural routing zones. Administrative workflows move smoothly from macro-system health down to low-level dynamic configurations.

## 1. Navigation Tree Matrix (`<SidebarNav/>`)

| Icon | Route | Purpose |
|------|-------|---------|
| 📊 | **Overview** | Central operational health check panel. Surfaces real-time telemetry lookups, geographic cluster hotspots, and global infrastructure totals. |
| 👥 | **Users** | Management view split cleanly between driver profiles (`role = 'driver'`) and partner organization accounts. |
| 📁 | **Data** | Parent-detail inventory context tracking the Partners Registry (categorized by entity classification: Business or Private), spatial Charging Stations, and physical Chargers terminal arrays. |
| 📈 | **Analytics** | Systems telemetry aggregator logs monitoring spatial coverage density patterns, performance baselines, and database load levels across Tunisian urban corridors. |
| 🛡️ | **Security** | Access boundary permission metrics (`<PermissionsMatrix/>`) and role audit grids. |
| ⚙️ | **Settings** | Form settings pattern split into two specialized sub-tracks (see below). |

### Settings Sub-Tracks

| Sub-Track | Scope |
|-----------|-------|
| **App Settings** | Core configuration profiles, branding parameters, image drag-and-drop file dropzones, and spatial map baseline tokens. |
| **Infrastructure Types** | Dynamic CRUD dictionary editor for connector variants (`CNT-`), populating dropdown input menus throughout the ecosystem. |

## 2. Cross-Workspace Configuration Dependency Topology

Dynamic infrastructure types created within the Settings track immediately update form options across the active Data management track:

```
SETTINGS PANEL ──► INFR. TYPES ──► Save Type ──► DB: station_connector_types
                                                    │
                                                    ▼ (Pulls dynamically)
DATA PANEL     ──► CHARGERS    ──► Form Entry ──► UI Dropdown: [Choose Type ▾]
```

### Dependency Flow

1. Admin creates or edits a connector type in **Settings → Infrastructure Types**
2. The type is persisted to `station_connector_types` table
3. The **Data → Chargers** form dynamically pulls available connector types from the same table
4. No hardcoded type lists exist — all dropdowns are database-driven

This ensures a single source of truth for connector type definitions and eliminates configuration drift between the Settings and Data panels.
