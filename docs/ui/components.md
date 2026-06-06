# Component Specifications

All components must be documented here before they are built. A component not in this document must not be built without updating this document first.

---

## Shared Components

Used by multiple applications (Driver Web, Driver Mobile, Dashboard).

### Button

**Purpose:** Primary interactive element  
**Variants:** primary, secondary, outline, ghost  
**Sizes:** sm, md, lg  
**States:** default, hover, active, disabled, loading

**Tokens Used:**
- Background: `brand.primary` (primary variant)
- Text: white or `text.main` (secondary)
- Border: `border.default` (outline variant)
- Radius: `rounded-lg`
- Padding: `px-lg py-md`

**Implementation Notes:**
- Primary variant: `bg-brand-primary text-white rounded-lg px-4 py-3 font-semibold`
- Disabled state: opacity-50, cursor-not-allowed
- Loading state: spinner icon, text hidden

### Input

**Purpose:** Text input for search, forms, filters  
**Variants:** text, email, password, search, number  
**Sizes:** sm, md, lg  
**States:** default, focus, error, disabled

**Tokens Used:**
- Background: `surface.card`
- Border: `border.default` (default), `brand.primary` (focus)
- Text: `text.main`
- Placeholder: `text.muted`
- Radius: `rounded-md`
- Padding: `px-md py-sm`

**Implementation Notes:**
- Focus border changes to `border-brand-primary` (2px width)
- Error state: border changes to `border-status-maintenance`
- Placeholder font-style: italic

### Select (Dropdown)

**Purpose:** Single or multi-select dropdown  
**Variants:** single, multi  
**States:** default, open, disabled, error

**Tokens Used:**
- Background: `surface.card`
- Border: `border.default`
- Chevron icon: `text.muted`
- Active item: `bg-brand-sageLight`
- Radius: `rounded-md`

### Checkbox

**Purpose:** Boolean input for forms  
**Sizes:** sm, md, lg  
**States:** unchecked, checked, disabled, indeterminate

**Tokens Used:**
- Background: `surface.card` (unchecked), `brand.primary` (checked)
- Border: `border.default`
- Checkmark: white
- Radius: `rounded-sm`

### Toggle

**Purpose:** On/off switch  
**Sizes:** sm, md  
**States:** off, on, disabled

**Tokens Used:**
- Background: `neutral.300` (off), `brand.primary` (on)
- Thumb: white
- Radius: `rounded-full`

### Textarea

**Purpose:** Multi-line text input for reviews, descriptions  
**States:** default, focus, error, disabled

**Tokens Used:**
- Same as Input (colors, border, padding)
- Min-height: typically 100px
- Resize: vertical only

### Modal

**Purpose:** Overlay dialog for important actions, login, forms  
**Variants:** default, fullscreen (mobile), centered  
**States:** open, closing

**Tokens Used:**
- Background: `surface.card`
- Overlay: `rgba(0, 0, 0, 0.5)` (dark semi-transparent)
- Border: `border.default` (optional)
- Radius: `rounded-2xl`
- Shadow: `shadow-float`
- Padding: `p-2xl`

**Implementation Notes:**
- Mobile: Fullscreen or bottom sheet (via `rounded-t-2xl`)
- Always has close button (X) in top-right
- Escape key closes modal

### Toast

**Purpose:** Temporary notification messages  
**Variants:** success, error, warning, info  
**Duration:** 3-5 seconds (auto-dismiss)

**Tokens Used:**
- Background: varies by variant (green, red, orange, blue)
- Text: white
- Radius: `rounded-lg`
- Shadow: `shadow-float`
- Padding: `p-lg`

**Implementation Notes:**
- Positioned: bottom-right (desktop), bottom-center (mobile)
- Auto-dismiss after 4 seconds
- Dismissible via close button

### Alert

**Purpose:** Persistent warning or info messages  
**Variants:** info, warning, error, success  
**Dismissible:** optional

**Tokens Used:**
- Background: light variant of status color
- Border-left: status color (3px)
- Text: `text.main`
- Icon: status color
- Padding: `p-lg`

### Badge

**Purpose:** Small label, tag, category indicator  
**Variants:** primary, secondary, outline, status  
**Sizes:** sm, md, lg

**Tokens Used:**
- Background: `brand-sageLight` (primary), `neutral.200` (secondary)
- Text: `text.main` (primary), `text.muted` (secondary)
- Radius: `rounded-full`
- Padding: `px-md py-xs`

**Status Badge Special Case:**
See `StatusBadge` below (status-specific badge).

### Skeleton

**Purpose:** Loading placeholder matching content shape  
**Variants:** text, heading, image, card

**Tokens Used:**
- Background: `neutral.200`
- Animation: pulse (fade in/out at 1.5s interval)
- Radius: matches component it's replacing

### EmptyState

**Purpose:** Message when no data exists  
**Variants:** search-no-results, no-favorites, no-reviews

**Tokens Used:**
- Icon: `text.muted`
- Heading: `text.main`
- Description: `text.muted`
- Radius: `rounded-lg` (for optional container)
- Padding: `p-2xl`

**Implementation Notes:**
- Always includes an SVG icon
- Optional CTA button below text

### ErrorState

**Purpose:** Error message for failed states  
**Variants:** network-error, server-error, not-found

**Tokens Used:**
- Icon: `status.maintenance` (#EF4444)
- Heading: `text.main`
- Description: `text.muted`
- Padding: `p-2xl`

**Implementation Notes:**
- Always includes retry button
- Optional support/contact information

### Table

**Purpose:** Tabular data display (Dashboard)  
**Features:** sorting, pagination, filtering, selection

**Tokens Used:**
- Header: `bg-brand-sageLight`, `text.main`, `font-semibold`
- Row: `bg-surface-card`, `border-b border-border-default`
- Striped: alternate rows with `bg-neutral-50`
- Hover: `bg-neutral-100`
- Padding: `p-md`

**Implementation Notes:**
- Header cells: `font-semibold text-xs uppercase tracking-wide`
- Borders between rows, not around cells
- Sortable headers show up/down chevron

### StatCard

**Purpose:** Single metric display (Dashboard)  
**Shows:** number, label, trend (up/down/neutral)

**Tokens Used:**
- Background: `surface.card`
- Border: `border.default`
- Stat number: `text.main`, `font-extrabold`, `text-2xl`
- Label: `text.muted`, `text-xs`
- Trend arrow: green (up), red (down), gray (neutral)
- Radius: `rounded-lg`
- Shadow: `shadow-card`
- Padding: `p-lg`

### StatusBadge

**Purpose:** Charger or station availability indicator  
**Variants:** available, in-use, maintenance, offline

**Tokens Used:**
- Available: `bg-status-availableBg`, `text-status-available`
- In Use: `bg-status-inUseBg`, `text-status-inUse`
- Maintenance: `bg-status-maintenanceBg`, `text-status-maintenance`
- Radius: `rounded-full`
- Padding: `px-md py-xs`
- Font: `font-semibold text-xs`

---

## Driver-Specific Components

Used only by Driver Web App and Driver Mobile App.

### MobileShell

**Purpose:** Root layout container for Driver Mobile App  
**Structure:** Full-bleed MapView with floating layers above it

**Layout:**
```
┌────────────────────────────────┐
│      MobileTopBar (floating)    │ z-10
├────────────────────────────────┤
│                                 │
│      MapView (full-bleed)       │ z-0
│      Background: #EAF0E6        │
│                                 │
│      SearchBar + FilterPills    │ z-20 (floating above map)
│      (floating above map)        │
│                                 │
│      BottomStationCard          │ z-30 (floating above all)
│      (floating bottom sheet)     │
│                                 │
├────────────────────────────────┤
│    BottomTabBar (fixed)         │ z-40 (above all)
└────────────────────────────────┘
```

**Tokens Used:**
- Background: `surface.background` (#F8FAF6)

**Implementation Notes:**
- Map fills entire screen (width: 100%, height: 100%)
- Safe area insets handled via platform-specific padding
- All interactive elements float above map with explicit z-index

### MobileTopBar

**Purpose:** Floating header over the map (Mobile only)  
**Contents:** Menu icon (left), brand name (center), notification bell (right)

**Layout:**
```
┌─────────────────────────────────────────────┐
│ ☰  BorneMap (brand.primary)        🔔(1)   │
└─────────────────────────────────────────────┘
```

**Tokens Used:**
- Background: `surface.card` with alpha (0.95 transparency)
- Border-bottom: `border.default`
- Text: `brand.primary` for logo text
- Icons: `text.main` (default), red dot on notification

**RTL Behavior:**
- Menu icon moves to right
- Notification bell moves to left
- Brand name stays centered

**Implementation Notes:**
- Height: 56px (Material Design standard)
- Padding accounts for iOS safe area (top inset)
- Notification badge: red dot, absolute positioned

### SearchBar

**Purpose:** Floating search input above the map (Driver Web + Mobile)  
**Placeholder:** "Search for stations..."

**Tokens Used:**
- Background: `surface.card`
- Border: `border.default`
- Focus border: `border-brand-primary`
- Icon: search icon, `text.muted`
- Placeholder: `text.muted` italic
- Radius: `rounded-xl`
- Padding: `px-lg py-md`
- Shadow: `shadow-card`

**RTL Behavior:**
- Search icon appears on the right side
- Input text aligns right
- Use `text-right` and CSS `direction: rtl`

### FilterPills

**Purpose:** Horizontal row of filter quick-action pills below search bar  
**Default Pills:** "Search Map", "Filters", "Nearby", "Available Only"

**Tokens Used:**
- Inactive: `bg-surface-card`, `border border-border-default`, `text-text-main`
- Active: `bg-brand-primary`, `text-white`, `border-brand-primary`
- Radius: `rounded-full`
- Padding: `px-lg py-sm`
- Font: `font-semibold text-sm`
- Spacing: `gap-sm` between pills

**RTL Behavior:**
- Pill row scrolls right-to-left
- Use `flex-row-reverse` in RTL or CSS `direction: rtl`

### MapPinMarker

**Purpose:** Station marker on the map  
**States:** default (available), selected (enlarged), unavailable

**Tokens Used:**
- Default state:
  - Size: 20px circle
  - Background: `brand.glow` (#00E676)
  - Border: 2px white
  - Shadow: `shadow.pin` (neon glow)
- Selected state:
  - Size: 28px circle
  - Same glow, larger
- Unavailable state:
  - Color: `status.maintenance` (#EF4444)
  - No glow shadow

**Implementation Notes:**
- SVG or CSS circle element
- Click opens/updates BottomStationCard
- Z-index: above map but below floating UI

### BottomStationCard

**Purpose:** Floating card anchored to bottom of screen showing station details  
**Content:** Station name, address, thumbnail, specs (charger count, rating, status), charger list

**Layout (Mobile):**
```
┌──────────────────────────────┐
│  STATIONS (section label)    │ uppercase tracking-widest, brand.primary, 10px
│  Station Name                 │ extrabold, base, text.main
│  123 Street, City             │ text-xs, text.muted
│                               │
│  [Thumbnail]  Specs:          │
│  [48×48]      • Power: 22kW   │
│               • Status: ⚡    │
│               • Rating: 4.8★  │
│                               │
│  (Swipe up to expand →)       │
└──────────────────────────────┘
```

**Tokens Used:**
- Background: `surface.card`
- Border: `border.default` (top only)
- Radius: `rounded-t-2xl` (top corners only)
- Shadow: `shadow.float`
- Padding: `p-lg`
- Gap between elements: `gap-md`

**Section Label Tokens:**
- Font: `uppercase tracking-widest font-bold text-xs`
- Color: `text-brand-primary`

**RTL Behavior:**
- Station name and address align right
- Thumbnail moves to left side
- SpecRow labels on right, values on left (justified across)
- Use `justify-between` — works correctly in both directions

**Implementation Notes:**
- Web: Sidebar or modal variant
- Mobile: Bottom sheet with swipe-to-expand gesture
- Click on card expands to full station detail screen

### SpecRow

**Purpose:** Single detail row inside BottomStationCard  
**Shows:** label and value in justified layout

**Layout:**
```
Label (left/RTL-right)              Value (right/RTL-left)
Power Output                        22 kW
Status                            ● Available
Rating                            ★★★★☆ 4.8
```

**Tokens Used:**
- Layout: `flex justify-between items-center`
- Label: `text-xs text-text-muted`
- Value: `text-xs text-text-main font-semibold`

**Special Case for Status:**
- Instead of plain text value, use `StatusBadge` component
- Gives visual emphasis to charger availability

**RTL Behavior:**
- Labels on right, values on left (in Arabic)
- `justify-between` handles this automatically

### ChargerRow

**Purpose:** Single charger display in station detail  
**Shows:** connector type, power output, availability status, last updated time

**Layout:**
```
Type 2 (CCS) 22 kW            ● Available (updated 2 min ago)
```

**Tokens Used:**
- Background: `surface-card` or `neutral-50` (alternating)
- Border: `border-b border-border-default`
- Padding: `p-md`
- Connector type: `font-semibold text-sm text-text-main`
- Power: `text-xs text-text-muted`
- Status: `StatusBadge` component
- Updated: `text-xs text-text-muted italic`

### ReviewCard

**Purpose:** Single review display on station detail  
**Shows:** user name, rating stars, comment, date

**Layout:**
```
┌─────────────────────────────────────┐
│ User Name              ★★★★★ (5.0)  │
│ Great station! Always available.     │
│                        Posted 3 days ago
└─────────────────────────────────────┘
```

**Tokens Used:**
- Background: `surface-card`
- Border: `border-b border-border-subtle`
- Padding: `p-md`
- Name: `font-semibold text-sm text-text-main`
- Stars: `text-yellow-500` or `text-amber-400`
- Comment: `text-sm text-text-main`
- Date: `text-xs text-text-muted italic`

### BottomTabBar

**Purpose:** Fixed bottom navigation for Driver Mobile App (Mobile only)  
**Tabs:** Home, [center action button], Settings

**Layout (LTR):**
```
┌──────────────────────────────────────────┐
│  🏠 Home        ╭─ + ─╮      ⚙️ Settings │
│  (active)       ╰─────╯      (inactive)  │
└──────────────────────────────────────────┘
```

**Tokens Used:**
- Background: `surface.card`
- Border-top: `border.default`
- Padding: accounts for iOS safe area (bottom inset)
- Tab height: 56px (Material Design standard)
- Active tab: `text-brand-primary`
- Inactive tab: `text-text-muted`
- Font: `font-bold text-xs`
- Icon: 24×24px

**RTL Behavior:**
- Tab order reverses in RTL (Settings on left, Home on right)
- Center action button stays centered regardless of direction

**Implementation Notes:**
- Fixed positioning at bottom of screen
- Never scrolls
- CenterActionButton overlaps between two tabs (see below)

### CenterActionButton

**Purpose:** Prominent circular action button raised above BottomTabBar  
**Action:** Opens nearby station quick view or triggers locate-me

**Layout:**
```
Button hovers above tab bar by 28px (56/2 + 8px offset)
```

**Tokens Used:**
- Background: gradient from `brand.primary` to `brand.primaryDark`
- Size: 56px circle (diameter)
- Icon: lightning bolt or location SVG, white, 24×24px
- Border: 4px solid `surface.background` (creates separation)
- Shadow: `shadow.float` (high elevation)
- Radius: `rounded-full`

**Implementation Notes:**
- Absolute positioned: `bottom: 4px (half tab height - border), centered x`
- Always rendered above BottomTabBar (z-index: higher)
- Touches both sides of BottomTabBar visually (4px border creates separation)
- Highlight/glow on press

### ZoomControls

**Purpose:** Floating button group for map zoom (Driver Web + Mobile)  
**Buttons:** + (zoom in), - (zoom out)

**Layout:**
```
┌───────┐
│   +   │
├───────┤
│   −   │
└───────┘
(positioned: bottom-right corner)
```

**Tokens Used:**
- Background: `surface.card`
- Border: `border.default`
- Top button: `rounded-t-lg`
- Bottom button: `rounded-b-lg`
- Button size: 44×44px
- Icon: `text.main`, centered
- Shadow: `shadow.card`
- Spacing: no gap between buttons (seamless)

**Implementation Notes:**
- Positioned: `bottom: 16px + BottomTabBar height (mobile), right: 16px`
- Hover/active: `bg-brand-sageLight`
- Mobile: accounts for safe area insets

### StationCard

**Purpose:** Card display of a single station (lists, search results)  
**Shows:** thumbnail, name, address, chargers, rating

**Layout (Horizontal):**
```
┌─────────────────────────────────────────┐
│ [Img] Station Name        ★★★★★ (4.8)  │
│       Address Line        22 kW • 3 chargers
│       [CTA buttons]                     │
└─────────────────────────────────────────┘
```

**Tokens Used:**
- Background: `surface.card`
- Border: `border.default`
- Radius: `rounded-lg`
- Shadow: `shadow.card`
- Padding: `p-md`
- Name: `font-bold text-base text-text-main`
- Address: `text-sm text-text-muted`
- Rating: `text-xs font-semibold`
- Gap: `gap-md`

**Thumbnail:**
- Size: 80×80px (web), 64×64px (mobile)
- Radius: `rounded-md`
- Placeholder: neutral background

---

## Dashboard-Specific Components

Used only by Dashboard App.

### AppShell

**Purpose:** Root layout for Dashboard App  
**Structure:** Sidebar + TopBar + PageContent

**Layout:**
```
┌────────┬─────────────────────────────┐
│        │     TopBar                  │ (with breadcrumbs, user menu)
│ Sidebar├─────────────────────────────┤
│        │                              │
│        │  PageContent                 │ (scrollable)
│        │  (main content area)         │
│        │                              │
│        │                              │
└────────┴─────────────────────────────┘
```

**Tokens Used:**
- Sidebar: `bg-surface-sidebar` (#FFFFFF), `border-r border-border-default`
- TopBar: `bg-surface-card`, `border-b border-border-default`
- PageContent: `bg-surface-background`

### Sidebar

**Purpose:** Navigation menu in Dashboard  
**Content:** App name, navigation items, user info (bottom)

**Tokens Used:**
- Background: `surface.sidebar` (#FFFFFF)
- Border: `border-r border-border-default`
- Logo: `text-brand-primary font-bold text-lg`
- Width: 256px (fixed)
- Padding: `p-lg`

**Items:**
- See `NavigationItem` below

### NavigationItem

**Purpose:** Single menu item in Sidebar  
**Variants:** default, active

**Tokens Used:**
- Inactive: `text-text-muted`, no background
- Active: `bg-brand-sageLight`, `text-brand-primary`, `font-semibold`
- Radius: `rounded-lg`
- Padding: `px-md py-sm`
- Font: `text-sm`

**Implementation Notes:**
- Icon + label layout
- Icon: 20×20px, left side
- Highlight entire item width on hover/active

### TopBar

**Purpose:** Header bar above main content  
**Content:** Breadcrumbs, page title, user menu

**Tokens Used:**
- Background: `surface.card`
- Border-bottom: `border.default`
- Padding: `p-lg`
- Title: `text.main font-bold text-2xl`
- Breadcrumb: `text-xs text-text-muted`

### PageContent

**Purpose:** Main content container  
**Features:** Scrollable, responsive grid layout

**Tokens Used:**
- Background: `surface.background`
- Padding: `p-2xl`
- Max-width: responsive (full on mobile, constrained on desktop)
- Gap: `gap-2xl` for vertical spacing

### DataCard

**Purpose:** Card for a single data point or metric  
**Shows:** label, number, optional chart/icon

**Tokens Used:**
- Background: `surface.card`
- Border: `border.default`
- Radius: `rounded-lg`
- Shadow: `shadow-card`
- Padding: `p-lg`

**Implementation Notes:**
- Often used in grid layout
- Similar to StatCard (shared component)

### DataTable

**Purpose:** Large data table with advanced features  
**Features:** sorting, filtering, pagination, row selection, export

**Tokens Used:**
- See Table component (shared) for base styling
- Header: `bg-brand-sageLight`
- Striped rows: alternating `bg-neutral-50`
- Hover: `bg-neutral-100`

**Implementation Notes:**
- Used for partners, stations, users, chargers lists
- Responsive: horizontal scroll on mobile

---

## Component Dependencies

### Shared Components Used By All Apps
- Button, Input, Select, Checkbox, Toggle, Textarea
- Modal, Toast, Alert, Badge, Skeleton, EmptyState, ErrorState
- StatusBadge

### Shared By Driver Apps
- Button, Input, Select, Checkbox, Toggle, Textarea
- Toast, Alert, Badge, Skeleton, EmptyState, ErrorState
- StatusBadge
- StationCard, ReviewCard, ChargerRow
- (And all shared components above)

### Shared By Web Apps (Driver Web + Dashboard)
- All shared components
- Table, StatCard (web-specific implementations)

### Dashboard Specific
- AppShell, Sidebar, NavigationItem, TopBar, PageContent, DataCard, DataTable
- Table (enhanced version)

---

## Component Build Order

**Phase 1 (Foundation):** Shared components
1. Button, Input, Select, Checkbox, Toggle, Textarea
2. Badge, Skeleton, EmptyState, ErrorState
3. Modal, Toast, Alert

**Phase 2 (Driver Features):**
4. StatusBadge, StationCard, ReviewCard, ChargerRow, SpecRow
5. MobileShell, MobileTopBar, SearchBar, FilterPills
6. MapPinMarker, BottomStationCard, BottomTabBar, CenterActionButton, ZoomControls

**Phase 3 (Dashboard):**
7. AppShell, Sidebar, NavigationItem, TopBar, PageContent
8. DataCard, DataTable
9. Table (enhanced with sorting/filtering)

**Phase 4 (Polish):**
10. StatCard, Skeleton (custom variants)
11. Animation refinements, RTL verification

---

## RTL Rules for Driver Apps

### MobileTopBar
- Menu icon moves to right
- Notification bell moves to left
- Brand name remains centered

### SearchBar
- Search icon appears on the right
- Input text aligns right
- Use `text-right` and `direction: rtl` on container

### FilterPills
- Pill row scrolls right-to-left
- Use `flex-row-reverse` in RTL or `direction: rtl` on container

### BottomStationCard
- Station name and address align right
- Thumbnail moves to left
- SpecRow labels on right, values on left
- `justify-between` works correctly in both directions

### BottomTabBar
- Tab order reverses in RTL
- CenterActionButton stays centered regardless of direction

### MapPinMarker
- No RTL change — geographic position is direction-agnostic

### ZoomControls
- Position on right side remains same in both LTR and RTL
- No text content, so no directional changes needed

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-05  
**Status:** Complete with Driver theme specifications
