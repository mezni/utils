# EV Charging Platform — CRUD Engine UI/UX Design Specification

> **Project**: BorneMap EV Infrastructure Platform  
> **Role**: Principal UI/UX Product Designer — Enterprise B2B Clean-Tech SaaS / Industrial IoT  
> **Stack**: React 18, TypeScript, TailwindCSS, Vite  
> **Data Hierarchy**: Partner (Asset Owner) → Station (Geospatial Location) → Charger (Physical EVSE Hardware)

---

## 1. Layout Architecture

### 1.1 Three-Panel Operational Workspace

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Top Bar: BorneMap logo · Breadcrumbs · Connection status · Panel toggle │
├──────────┬──────────────┬──────────────────────────────────────────────┤
│          │              │                                              │
│ PARTNER  │  STATION     │  CHARGER DETAIL / TABLE                     │
│ LIST     │  CONTEXT     │                                              │
│          │              │  ┌─────────────────────────────────────────┐│
│ ┌──────┐ │  ┌────────┐ │  │  Connector Panel: CCS2 | CHADEMO |     ││
│ │ACME  │ │  │Hbf Hub│ │  │  TYPE2  (live state per port)           ││
│ │Volt  │ │  │Garage │ │  ├─────────────────────────────────────────┤│
│ │Charge│ │  │Airport│ │  │  Telemetry: kW · V · A · uptime %       ││
│ │Power │ │  │       │ │  ├─────────────────────────────────────────┤│
│ │Sun   │ │  │       │ │  │  Config: OCPP · Firmware · RevShare %   ││
│ │Eco   │ │  │       │ │  └─────────────────────────────────────────┘│
│ │City  │ │  │       │ │                                              │
│ └──────┘ │  └────────┘ │                                              │
│          │              │                                              │
│ w-60     │  w-72       │  flex-1 (max-w-7xl)                          │
├──────────┴──────────────┴──────────────────────────────────────────────┤
│  Context-preserving: selecting a partner shows their stations          │
│  in the middle panel. Selecting a station shows charger table          │
│  in main panel. Selecting a charger shows detail view.                 │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Component Architecture

| Component | Role | Width | Key Features |
|-----------|------|-------|-------------|
| `AppShell` | Root layout shell | 100vw | Top bar + 3-column flex layout |
| `Sidebar` | Partner navigation | 240px (w-60) | Active indicator glow, count badges, scrollable |
| `ContextBar` | Station context | 288px (w-72) | Station list, partner details, edit/delete actions |
| `BreadcrumbNav` | Hierarchical crumbs | auto | Clickable back-navigation through hierarchy |
| `Main` | Charger table/detail | flex-1 | Max width 1280px, bg-grid subtle |

---

## 2. Advanced "Read" UI Engine (Data Tables)

### 2.1 Dynamic Column Structures

#### Partner Table

| Column | Type | Width | Render | Notes |
|--------|------|-------|--------|-------|
| ID | `string` | 96px | Mono `#A1B2C3D4E5F6` | Truncated, text-gray-500 |
| Name | `string` | flex-2 | `font-medium` | Primary link |
| Status | `EntityStatus` | 96px | `StatePill` | Color-coded: green/red/yellow/blue/purple |
| Stations | `number` | 80px | Mono `tabular-nums` | Aggregate count |
| Chargers | `number` | 80px | Mono `tabular-nums` | Aggregate count |
| Total kW | `number` | 96px | Mono `text-orange-400` | Formatted with locale |
| Telemetry | `Telemetry` | 192px | `TelemetryMini` | kW + uptime% compact |

#### Station Table

| Column | Type | Width | Render | Notes |
|--------|------|-------|--------|-------|
| ID | `string` | 80px | Mono small | |
| Name | `string` | flex-2 | `font-medium` | |
| Status | `EntityStatus` | 96px | `StatePill` | |
| Load | `number` | 112px | kW + progress bar | Bar turns yellow >80% |
| Active | `string` | 64px | `3/4` charger ratio | |
| Telemetry | `Telemetry` | 192px | `TelemetryMini` | |

#### Charger Table

| Column | Type | Width | Render | Notes |
|--------|------|-------|--------|-------|
| ChargeBox ID | `string` | 144px | Mono `CP-ABC-001` | |
| Model | `string` | 128px | `ABB Terra 350` | Manufacturer + model |
| Status | `ChargerState` | 96px | `StatePill` with pulse | Pulsing dot for active/charging |
| Power | `number` | 80px | `350` kW | Mono orange |
| Session | `number\|null` | 80px | `42.5 kWh` or `—` | Blue when active |
| OCPP | `string` | 64px | `2.0.1` | Hidden on mobile |
| RevShare | `number` | 80px | `5.0%` | Hidden on mobile |
| Telemetry | `Telemetry` | 176px | `TelemetryMini` | |

### 2.2 State Pills (System State)

```
┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐
│ ● ACTIVE │  │ ● FAULTED│  │●THROTTLED│  │●CHARGING │  │ ● OFFLINE│  │●MAINTENAN│
│ green    │  │ red      │  │ yellow   │  │ blue     │  │ gray     │  │ purple   │
└──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘  └──────────┘
  bg-green/10  bg-red/10    bg-yellow/10  bg-blue/10    bg-gray/10   bg-purple/10
  text-green4  text-red-400 text-yel-400  text-blue-400 text-gray400 text-purple4
```

Active and Charging states display an animated pulse dot (`animate-pulse-dot`).

### 2.3 Telemetry Micro-Metrics

Compact (`TelemetryMini compact`):

```
452.3kW · 97.2%
```

Full (`TelemetryMini`):

```
kW      V       A       UP
452.3   408     1109    97.2%
```

Color coding:
- **kW**: `text-orange-400`
- **V**: `text-blue-400`
- **A**: `text-yellow-400`
- **Uptime**: `text-green-400` (>99%), `text-yellow-400` (>95%), `text-red-400` (<95%)

### 2.4 Hierarchical Navigation & Context Preservation

**Navigation model**:
1. User clicks Partner in sidebar → ContextBar shows stations list, main panel shows station table
2. User clicks Station in ContextBar → Main panel shows charger table, ContextBar highlight updates
3. User clicks Charger in main panel → Full detail view replaces table, breadcrumb shows chain

**Context preservation**:
- Sidebar partner selection remains highlighted throughout drill-down
- ContextBar remains visible and shows parent station info when charger is selected
- BreadcrumbNav provides clickable path: `Partner › Station › Charger`
- Returning to a parent via breadcrumb or sidebar click restores the appropriate view

```
[Partner: GreenCharge Networks]  ›  [Station: Berlin Hbf Hub]  ›  [Charger: CP-ABC-001]
```

### 2.5 Micro-Interactions

| Interaction | Behavior | Timing |
|------------|----------|--------|
| Row hover | `bg-gray-800/40` background | 100ms transition |
| Quick Actions menu | 3-dot icon appears on row hover, `opacity 0→1` | 100ms delay on unhover |
| Click row | Navigate to child entity | Instant |
| Sidebar item click | Orange active glow + left border `shadow-[0_0_8px_rgba(249,115,22,0.5)]` | 150ms |
| Panel transitions | `animate-slide-up` (300ms ease-out) | On data change |
| Quick Actions dropdown | `animate-scale-in origin-top-right` | 200ms |

---

## 3. Complex Form UX (Create & Update Engine)

### 3.1 Form Patterns

#### 600px Sliding Context Drawer

Used for: Quick edits, single-entity creation

```
┌────────────────────────────────────┐
│  ✕  Create Charger                 │
│     Add a new charger to network    │
├────────────────────────────────────┤
│                                    │
│  OCPP Configuration                │
│  ┌──────────────┐ ┌──────────────┐│
│  │ ChargeBox ID  │ │ OCPP Version ││
│  │ CP-ABC-001    │ │ 2.0.1        ││
│  └──────────────┘ └──────────────┘│
│  ┌──────────────────────────────┐ │
│  │ Serial Number                │ │
│  │ SN-2A3B4C5D                  │ │
│  └──────────────────────────────┘ │
│                                    │
│  Hardware Profile                  │
│  ┌──────────────┐ ┌──────────────┐│
│  │ Manufacturer  │ │ Model        ││
│  │ ABB           │ │ Terra 350    ││
│  └──────────────┘ └──────────────┘│
│  Max Power: [350] kW              │
│  Connectors: [CCS2] [TYPE2] [NACS]│
│                                    │
│  Financial Configuration           │
│  RevShare [5.0] % · Rate [0.35]   │
│  Payout: 0xABCD...                 │
│                                    │
├────────────────────────────────────┤
│  [Cancel]           [Create]      │
└────────────────────────────────────┘
  `animate-slide-in-right` (300ms cubic-bez)
```

#### Multi-Step Provisioning Wizard

Used for: New hardware provisioning, station deployment

```
Step 1: Identity    Step 2: Hardware    Step 3: Financial    Step 4: Review
    ○───────●───────○────────────────────○───────────────────○
     ✓      ⬡      ⬡                    ⬡                   
   [Identity] [Hardware] [Financial] [Review]

Step content area (bg-surface, border, rounded-xl, min-h-[300px]):

┌────────────────────────────────────────────────┐
│                                                │
│  Current step form content with validation      │
│                                                │
│                                                │
└────────────────────────────────────────────────┘

[Cancel]                    [Back]  [Continue]
```

### 3.2 Technical Input Controls

#### Geospatial MapPicker

```
┌──────────────────────────────────────┐
│  Latitude          Longitude          │
│  ┌──────────────┐  ┌──────────────┐  │
│  │ 48.8566      │  │ 2.3522       │  │
│  └──────────────┘  └──────────────┘  │
│                                      │
│       ┌────────────────────┐         │
│       │    ╲│╱             │         │
│       │   ──(+)──          │         │
│       │    ╱│╲             │         │
│       │  48.8566°, 2.3522° │         │
│       └────────────────────┘         │
│  Interactive map requires API key    │
└──────────────────────────────────────┘
```

- Crosshair overlay with coordinate display
- Grid background pattern
- Manual decimal-degree input with validation (±90°, ±180°)

#### OCPP Configuration

| Field | Type | Validation | Format |
|-------|------|-----------|--------|
| ChargeBox ID | `text` | Regex `^CP-[A-Z0-9]+-\d{3}$` | CP-PREFIX-001 |
| OCPP Version | `text` | Enum `1.6\|2.0.1` | Selectable |
| Serial Number | `text` | Alphanumeric | SN-XXXXXX |

#### Hardware Profile

- **Manufacturer**: Free text with autocomplete (ABB, Siemens, Delta, Alpitronic, Tesla)
- **Model**: Free text
- **Max Power**: Number input (50–1000 kW), integer validation
- **Max Connectors**: Number input (1–6)
- **Connector Types**: Toggle-chips (CCS2, CHADEMO, TYPE2, GBT, NACS) — multi-select

#### Grid Limit Calculator

```
Grid Limit [1000] kW         Total Load [1050] kW

Utilization: ████████████████████░░░  105%

⚠ Grid limit exceeded
Total charger load (1050 kW) exceeds grid capacity (1000 kW).
Increase grid limit or reduce charger count.
```

- Real-time bar visualization
- Color-coded: green (safe >20% headroom), yellow (<20% headroom), red (exceeded)
- Warning banner with actionable message

#### Financial Splits

| Field | Type | Validation | Notes |
|-------|------|-----------|-------|
| Revenue Share | `number 0-100` | Step 0.5 | Partner payout percentage |
| Energy Rate | `number` | 3 decimal places | €/kWh |
| Payout Address | `text` | ETH address or IBAN | Auto-detected format |
| Tariff ID | `text` | Alphanumeric | Reference to tariff table |

### 3.3 Real-Time Guardrails & Validation

| Rule | Component | Message |
|------|-----------|---------|
| Duplicate OCPP ID | `OcppConfigFields` | "ChargeBox ID already exists in the network" |
| Grid limit exceeded | `GridLimitCalculator` | "Grid limit ({n} kW) is below total charger capacity ({m} kW)" |
| Power rating too low | `ValidationGuardrail` | "Minimum 50 kW for DC fast charging" |
| Power rating too high | `ValidationGuardrail` | "Maximum 1000 kW per charger" |
| Invalid payout format | `FinancialSplitFields` | "Must be a valid ETH address (0x...) or IBAN" |
| Invalid tax ID format | Partner form | "Format must be DE123456789 (Germany)" per-country match |
| Maximum grid headroom warning | `GridLimitCalculator` | "Recommended minimum headroom is 20% ({n} kW)" |

All guardrails appear as animated banners below the relevant input:

```
┌────────────────────────────────┐
│  Input field                   │
├────────────────────────────────┤
│ ⚠ Warning/error message here   │  ← animate-slide-up
│   (bg-{type}-500/5 border)     │
└────────────────────────────────┘
```

---

## 4. Decommissioning & Safety Frameworks (Delete UX)

### 4.1 Soft-Delete & Safe Dependency Checks

When a user attempts to delete a **Partner** or **Station** with active children:

```
┌──────────────────────────────────────────────────┐
│  ✕  Cannot Delete Partner                        │
│                                                   │
│  ⚠ Active Dependencies Found                     │
│  "GreenCharge Networks" has 12 active stations    │
│  assigned. Deleting this partner will orphan      │
│  these assets.                                    │
│                                                   │
│  ┌──────────────────────────────────────────────┐ │
│  │ Recommended: Reassign Dependencies           │ │
│  │ Transfer stations to another partner before  │ │
│  │ deletion to preserve continuity.             │ │
│  │                                              │ │
│  │ [Reassign 12 Stations]                       │ │
│  └──────────────────────────────────────────────┘ │
│                                                   │
│  Destructive options (not recommended):           │
│  [Force Delete — Cascade to 12 Stations]          │
│  ↑ Red button, secondary action                   │
│                                                   │
│                                  [Cancel]         │
└──────────────────────────────────────────────────┘
```

**Flow**:
1. User clicks "Delete" → DependencyCheckModal opens
2. Yellow warning with active dependency count
3. Primary CTA: "Reassign Dependencies" (opens reassign workflow)
4. Secondary CTA: "Force Delete" (red, cascading delete)
5. Cancel returns to table

### 4.2 Graceful Charger Unbinding

```
┌──────────────────────────────────────────────────┐
│  ✕  Unbind & Archive Charger                     │
│                                                   │
│  ⚡ Unbind Charger                                │
│  This will immediately remove charger             │
│  CP-ABC-001 from all OCPI roaming maps and        │
│  archive it. Historical billing data is           │
│  preserved.                                       │
│                                                   │
│  ┌──────────────┬─────────────────────────────┐   │
│  │CHR-A1B2C3D4E5│ State: ● ACTIVE             │   │
│  │CP-ABC-001    │ Station: Berlin Hbf Hub      │   │
│  └──────────────┴─────────────────────────────┘   │
│                                                   │
│  If session active:                               │
│  ⓘ Session will be allowed to finish naturally.  │
│    Unbind processes at session end.               │
│                                                   │
│  Step 2: Type confirmation string to proceed      │
│  ┌──────────────────────────────────────────────┐ │
│  │ CP-ABC-001                                   │ │
│  └──────────────────────────────────────────────┘ │
│                                                   │
│  Step 3: Confirmation ✓ Unbind complete            │
│                                                   │
│                                   [Cancel]  [Unbind]│
└──────────────────────────────────────────────────┘
```

**Three-step flow**:
1. **Review**: Shows entity details, warns of session
2. **Confirm**: User must type the ChargeBox ID to confirm
3. **Complete**: Success confirmation with green check

**Session handling**:
- If `charger_state === 'CHARGING'`: Unbind is queued, auto-executes on session end
- Blue info banner explains the delay

### 4.3 Failsafe Confirmation Patterns

#### High-Friction Confirmation

For destructive physical commands (Remote Reboot, Firmware Update, etc.):

| Layer | Mechanism | Example |
|-------|-----------|---------|
| 1 | String match | Type `CP-ABC-001` to confirm |
| 2 | Manager approval | Select approving manager from dropdown |
| 3 | PIN authorization | 6-digit manager PIN (simulated: `424242`) |

#### Two-Step Manager Approval

```
Step 1: Request              Step 2: Verify
┌────────────────────┐       ┌────────────────────┐
│ Request Approval    │       │ Manager PIN         │
│                     │       │                     │
│ Command: Remote     │       │ Command: Remote     │
│   Reboot            │       │   Reboot            │
│ Target: CP-ABC-001  │       │ Reason: Firmware    │
│ Reason: [textarea]  │       │   update failed     │
│                     │       │                     │
│ [Cancel] [Request]  │       │ PIN: [●●●●●●]      │
│                     │       │ [Reject] [Authorize]│
└────────────────────┘       └────────────────────┘
```

---

## 5. Component Inventory

| Category | Component | File | Lines | Dependencies |
|----------|-----------|------|-------|-------------|
| **UI Primitives** | Button | `ui/Button` | 41 | — |
| | Input | `ui/Input` | 38 | — |
| | Modal | `ui/Modal` | 49 | — |
| | Badge | `ui/Badge` | 22 | types |
| **Data Display** | HyperTable | `data/HyperTable` | 112 | StatePill, TelemetryMini, QuickActions |
| | StatePill | `data/StatePill` | 27 | types |
| | TelemetryMini | `data/TelemetryMini` | 53 | types |
| | QuickActions | `data/QuickActions` | 56 | Button |
| **Forms** | SlideDrawer | `forms/SlideDrawer` | 53 | — |
| | ProvisionWizard | `forms/ProvisionWizard` | 92 | Button |
| | MapPicker | `forms/MapPicker` | 58 | Input |
| | OcppConfigFields | `forms/OcppConfigFields` | 48 | Input |
| | HardwareProfileFields | `forms/HardwareProfileFields` | 92 | Input |
| | FinancialSplitFields | `forms/FinancialSplitFields` | 70 | Input |
| | GridLimitCalculator | `forms/GridLimitCalculator` | 96 | Input |
| | ValidationGuardrail | `forms/ValidationGuardrail` | 56 | — |
| **Safety** | DependencyCheckModal | `safety/DependencyCheckModal` | 68 | Modal, Button |
| | UnbindArchiveFlow | `safety/UnbindArchiveFlow` | 130 | Modal, Button, StatePill |
| | FailsafeConfirm | `safety/FailsafeConfirm` | 117 | Modal, Button |
| | TwoStepApproval | `safety/TwoStepApproval` | 110 | Modal, Button |
| **Layout** | AppShell | `layout/AppShell` | 65 | — |
| | Sidebar | `layout/Sidebar` | 49 | — |
| | ContextBar | `layout/ContextBar` | 38 | — |
| | BreadcrumbNav | `layout/BreadcrumbNav` | 24 | — |
| **Pages** | DirectoryPage | `pages/DirectoryPage` | ~450 | All of the above |
| **Data** | Mock | `data/mock` | ~220 | types |

---

## 6. Design System Tokens

### Colors (Dark Mode OLED)

| Token | Hex | Usage |
|-------|-----|-------|
| `background` | `#020617` | Page background |
| `foreground` | `#F8FAFC` | Text on dark |
| `surface` | `#0F172A` | Card/panel background |
| `surfaceAlt` | `#1A1E2F` | Elevated surface |
| `muted` | `#334155` | Muted text/separators |
| `border` | `#1E2938` | Borders/dividers |
| `primary` | `#F97316` | CTAs, highlights |
| `accent` | `#22C55E` | Success, active states |
| `destructive` | `#EF4444` | Danger, deletion |
| `warning` | `#FBBF24` | Warnings, throttled |
| `info` | `#3B82F6` | Charging, info banners |

### Typography

- **Headings**: `Fira Code` (monospace) — font-mono
- **Body**: `Fira Sans` (sans-serif) — font-sans
- **Data**: `Fira Code` / tabular-nums for telemetry and IDs

### Animation

| Name | Duration | Easing | Usage |
|------|----------|--------|-------|
| `slideUp` | 300ms | ease-out | Content transitions |
| `fadeIn` | 200ms | ease-out | Modal overlays |
| `slideInRight` | 300ms | cubic-bezier(0.16,1,0.3,1) | Drawer |
| `slideInLeft` | 250ms | ease-out | ContextBar |
| `scaleIn` | 200ms | ease-out | Modals, dropdowns |
| `pulseDot` | 2s | ease-in-out | Live status indicators |

---

## 7. File Structure

```
apps/test-dashboard/src/
├── main.tsx
├── App.tsx
├── index.css                          # Design tokens, animations, utilities
├── types/
│   ├── index.ts
│   ├── common.ts                      # BaseEntity, TelemetrySnapshot, ConnectorState
│   ├── partner.ts                     # Partner, CreatePartnerRequest, UpdatePartnerRequest
│   ├── station.ts                     # Station, CreateStationRequest, UpdateStationRequest
│   └── charger.ts                     # Charger, CreateChargerRequest, UpdateChargerRequest
├── data/
│   └── mock.ts                        # 8 partners, 25 stations, ~120 chargers
├── components/
│   ├── ui/
│   │   ├── Button.tsx
│   │   ├── Input.tsx
│   │   ├── Modal.tsx
│   │   └── Badge.tsx
│   ├── data/
│   │   ├── HyperTable.tsx             # High-density table with columns/rows/expanders
│   │   ├── StatePill.tsx              # Color-coded system state indicator
│   │   ├── TelemetryMini.tsx          # Compact telemetry readout
│   │   └── QuickActions.tsx           # Row-level action dropdown
│   ├── forms/
│   │   ├── SlideDrawer.tsx            # 600px right slide drawer
│   │   ├── ProvisionWizard.tsx        # Multi-step wizard shell
│   │   ├── MapPicker.tsx              # Coordinate input + visual map placeholder
│   │   ├── OcppConfigFields.tsx       # OCPP identity fields
│   │   ├── HardwareProfileFields.tsx   # Power/connector configuration
│   │   ├── FinancialSplitFields.tsx   # Revenue/tariff fields
│   │   ├── GridLimitCalculator.tsx     # Grid capacity visualization
│   │   └── ValidationGuardrail.tsx    # Inline validation messages + rules
│   ├── safety/
│   │   ├── DependencyCheckModal.tsx   # Active dependency warning
│   │   ├── UnbindArchiveFlow.tsx      # 3-step unbind flow
│   │   ├── FailsafeConfirm.tsx        # String-match + manager approval
│   │   └── TwoStepApproval.tsx        # Request → Verify approval flow
│   └── layout/
│       ├── AppShell.tsx               # 3-column layout shell
│       ├── Sidebar.tsx                # Left partner navigation
│       ├── ContextBar.tsx             # Station context panel
│       └── BreadcrumbNav.tsx          # Hierarchical breadcrumbs
└── pages/
    └── DirectoryPage.tsx              # Main page orchestrating all components
```

---

## 8. Key Design Decisions

1. **3-panel layout**: Sidebar (partners) → ContextBar (stations) → Main (chargers) preserves the physical hierarchy visually and prevents disorientation during deep navigation.

2. **Quick Actions on hover**: Reducing visual clutter by hiding actions until row hover keeps the table clean while maintaining power-user efficiency.

3. **600px drawer for edits**: Drawer pattern (rather than full-page form) preserves operational context — users can still see the table behind the overlay.

4. **Multi-step wizard for provisioning**: Breaking complex charger provisioning into discrete steps (Identity → Hardware → Financial → Review) prevents errors and guides operators through required fields.

5. **3-layer safety confirmation**: String match → Manager approval → PIN verification ensures that destructive operations on live electrical infrastructure cannot happen accidentally.

6. **State pills over text badges**: Color-coding system states with semantic colors (green=active, red=faulted, yellow=throttled, blue=charging) enables instant visual scanning of infrastructure health.

7. **Telemetry micro-metrics in table rows**: Embedding live kW, uptime %, and voltage data directly in table rows eliminates the need to navigate to detail views for operational awareness.

8. **In-memory mock data layer**: Self-contained mock data (8 partners, 25 stations, ~120 chargers) enables full CRUD testing without backend dependency.

---

*Specification v1.0 — Principal UI/UX Product Designer*  
*Implementation: `apps/test-dashboard` — `npm run dev` on port 5180*
