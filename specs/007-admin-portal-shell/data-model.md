# Data Model: Admin Portal — Shell, Navigation & BaseMap

## Component Hierarchy

```
<App>
  └── <AppShell>
        ├── <Header>
        │     └── <SandboxToggle />        — localStorage-backed toggle
        ├── <SidebarNav>
        │     ├── <NavItem label="Overview"   icon icon="chart-bar"    route="/" />
        │     ├── <NavItem label="Users"      icon icon="users"        route="/users" />
        │     ├── <NavItem label="Data"       icon icon="database"     route="/data" />
        │     ├── <NavItem label="Analytics"  icon icon="trending-up"  route="/analytics" />
        │     ├── <NavItem label="Security"   icon icon="shield"       route="/security" />
        │     └── <NavItem label="Settings"   icon icon="cog"          route="/settings" />
        └── <main>  ← React Router <Outlet />
              ├── Overview      → <OverviewDashboard />
              ├── Users         → <UsersPage />           (placeholder)
              ├── Data          → <DataPage />             (placeholder — Phase 4)
              ├── Analytics     → <AnalyticsPage />        (placeholder)
              ├── Security      → <SecurityPage />         (placeholder)
              └── Settings      → <SettingsPage />         (placeholder)

<OverviewDashboard>
  ├── <MetricChip label="Total Stations"       value={number} />
  ├── <MetricChip label="Total Chargers"       value={number} />
  ├── <MetricChip label="Total Partners"       value={number} />
  ├── <BaseMap>
  │     └── <StationMarker
  │             position={[lat, lng]}
  │             stationId={string}
  │             name={string}
  │             city={string}
  │             chargerCount={number}
  │           />  ← click → popup with name, city, charger count + "View Details"
  └── <AnalyticsPlaceholderCards />           (post-MVP0)
```

## Props Interfaces

```typescript
// ── Layout ──

interface AppShellProps {
  children: ReactNode;  // Rendered via Outlet
}

interface SidebarNavProps {
  activeRoute: string;
  onNavigate: (route: string) => void;
}

interface NavItemProps {
  label: string;
  icon: string;         // Icon identifier
  route: string;
  isActive: boolean;
  onClick: () => void;
}

interface HeaderProps {
  isSandboxActive: boolean;
  onSandboxToggle: (active: boolean) => void;
}

// ── Map ──

interface BaseMapProps {
  stations: StationMarkerData[];
  center?: [number, number];  // Default: [33.8869, 9.5375]
  zoom?: number;               // Default: 7
}

interface StationMarkerData {
  id: string;             // e.g., "STN-k4m2n9p1q5v8"
  name: string;
  city: string;
  coordinates: [number, number];  // [lng, lat]
  chargerCount: number;
}

// ── Overview Dashboard ──

interface MetricChipProps {
  label: string;
  value: number | null;   // null = loading (show skeleton)
  isLoading?: boolean;
}

interface OverviewDashboardProps {
  stationCount: number;
  chargerCount: number;
  partnerCount: number;
  stations: StationMarkerData[];
  isLoading: boolean;
}

// ── Design System Components ──

interface SettingsCardProps {
  title: string;
  description?: string;
  children: ReactNode;
}

interface SelectSettingProps {
  label: string;
  options: { value: string; label: string }[];
  value: string;
  onChange: (value: string) => void;
}

interface ConfirmDeleteModalProps {
  isOpen: boolean;
  resourceId: string;     // The actual ID to match (e.g., "STN-4f7d2a8b9c02")
  resourceLabel: string;  // Human-readable label (e.g., "Station ABC")
  onConfirm: () => void;
  onCancel: () => void;
}
```

## State Shape

```typescript
// ── Global State (React Context) ──

interface AppState {
  isSandboxActive: boolean;
  setSandboxActive: (active: boolean) => void;
}

// ── Component State ──

interface OverviewDashboardState {
  stationCount: number | null;
  chargerCount: number | null;
  partnerCount: number | null;
  stations: StationMarkerData[];
  isLoading: boolean;
  error: string | null;
}

// ── localStorage Key ──

const SANDBOX_STORAGE_KEY = 'bornemap_admin_sandbox';
```

## API Response Shapes

```typescript
// GET /api/v1/stations (list)
// Response shape consumed by Overview Dashboard and BaseMap

interface StationListItem {
  id: string;           // "STN-..."
  name: string;
  city: string;
  latitude: number;
  longitude: number;
  is_operational: boolean;
  // other fields ignored by Phase 3
}

interface PaginatedResponse<T> {
  data: T[];
  total: number;
  // pagination fields as needed
}

// Derived counts
interface DashboardMetrics {
  totalStations: number;
  totalChargers: number;
  totalPartners: number;
}
```

## Validation Rules

| Component | Rule | Source |
|-----------|------|--------|
| MetricChip | Value displays 0 when null/undefined | FR-009 |
| MetricChip | Shows skeleton placeholder when isLoading = true | FR-013 |
| ScrollableTable | Minimum content width 800px, horizontal scroll | FR-006 |
| ConfirmDeleteModal | Confirm button disabled until input matches resourceId exactly | FR-008 |
| ConfirmDeleteModal | Matching is case-sensitive, full ID required | docs/06-defensive-ux-guardrails.md |
| SandboxToggle | State persisted in localStorage | FR-004 / Clarification Q3 |
| SidebarNav | Active route highlighted, URL updates on navigation | FR-003 |
