# Screen Specifications

All screens and user flows must be documented here before implementation. A screen not in this document must not be built without updating this document first.

---

## Driver Web App Screens

Map-centric web experience with responsive layout.

### Home / Map Screen

**Purpose:** Primary discovery interface  
**Layout:** Full-bleed map + floating UI elements

**Components:**
- MobileTopBar (web variant: horizontal header)
- SearchBar (floating above map)
- FilterPills (row below search)
- MapPinMarker (stations on map)
- ZoomControls (bottom-right)
- BottomStationCard (web variant: right sidebar or modal)

**Key Features:**
- Auto-center map on user location (if permitted)
- Map shows all stations by default
- Click station pin → updates BottomStationCard
- Search updates visible pins
- Filter pills toggle filter criteria

**Tokens:**
- Map background: `surface.mapTerrain` (#EAF0E6)
- UI overlays: `surface.card`
- Active elements: `brand.primary`

### Station Detail Screen

**Purpose:** In-depth view of a single station  
**Layout:** Two-column (map on left, details on right) or modal overlay

**Components:**
- Station header (name, address, rating)
- SpecRow list (power output, charger count, operating hours)
- ChargerRow list (each charger with status and specs)
- StatusBadge (charger availability)
- ReviewCard list (user reviews with ratings)
- CTA buttons (Favorite, Write Review, Share)

**Key Features:**
- Map preview showing station location
- Charger list with real-time availability
- Reviews sorted by date (newest first)
- Moderation badge for flagged reviews
- Favorite button state shows/hides heart icon
- Share button opens native share dialog

**Tokens:**
- Background: `surface.background`
- Cards: `surface.card`
- Headings: `text.main font-extrabold`
- Subtext: `text.muted`

### Search Results Screen

**Purpose:** Display filtered station list  
**Layout:** List with search/filter controls at top

**Components:**
- SearchBar (sticky at top)
- FilterPills (sticky below search)
- StationCard list (scrollable)
- EmptyState (if no results)
- Pagination or infinite scroll

**Key Features:**
- Search term highlighted in results
- Sort options: distance, rating, updated
- Results count ("42 stations found")
- Clear all filters button
- Swipe/click card to go to detail

**Tokens:**
- Results container: `surface.background`
- Cards: `surface.card with shadow-card`

### Login / Register Screen

**Purpose:** Authentication entry point  
**Layout:** Centered card over blurred map background

**Components:**
- Logo (centered, `brand.primary`)
- Email Input
- Password Input
- Sign In Button (primary)
- Social Login Buttons (Google, Facebook)
- "Create Account" link
- "Forgot Password" link

**Key Features:**
- Form validation (client-side)
- Loading spinner on button during submission
- Error Toast on failed login
- Successful login redirects to Home screen
- Tab between fields with keyboard

**Tokens:**
- Modal background: `surface.card`
- Button: `brand.primary`
- Links: `text-brand-primary`
- Error: `status.maintenance`

**Registered Upgrade Flow:**
- Public user clicks "Favorite"
- Modal appears with sign-in options
- After login, original action (favorite) resumes automatically

### Favorites Screen

**Purpose:** View saved stations  
**Layout:** List of cards

**Components:**
- SearchBar (optional, for filtering favorites)
- StationCard list (each card in favorites)
- EmptyState ("You haven't saved any stations yet")
- "Explore Stations" CTA button in empty state

**Key Features:**
- Favorite indicator always shows (full heart)
- Click heart to remove from favorites
- Sort by: date added, rating, distance
- Click card to go to detail

**Tokens:**
- Heart icon: `brand.primary` (filled)

### Profile Screen

**Purpose:** User account settings  
**Layout:** Form-based, centered or sidebar

**Components:**
- Avatar (editable, click to upload)
- Name Input (first + last)
- Email Input (read-only, no change allowed)
- Phone Input (optional)
- Language Select (French, Arabic, English)
- Save Button
- Logout Button (danger styling)

**Key Features:**
- Avatar upload with image cropping
- Form validation on all inputs
- Success Toast on save
- Logout clears JWT token and redirects to home

**Tokens:**
- Form inputs: `surface.card` background
- Labels: `text.muted text-xs`
- Save button: `brand.primary`
- Logout button: red/`status.maintenance`

---

## Driver Mobile App Screens

Mobile-first experience with bottom sheet patterns.

### Map / Home Screen

**Purpose:** Primary mobile discovery  
**Layout:** Full-bleed map + floating elements + bottom tab bar

**Components:**
- MobileShell (root layout)
- MobileTopBar (floating header)
- SearchBar (floating, drag-dismissible)
- FilterPills (below search)
- MapPinMarker (on map)
- BottomStationCard (floating, swipe-to-expand)
- BottomTabBar (fixed at bottom)
- CenterActionButton (raised button in tab bar)
- ZoomControls (right side)

**Key Features:**
- Default: shows all stations
- Tap station pin → BottomStationCard updates
- Drag BottomStationCard up → expands to full detail
- Drag BottomStationCard down → collapses to preview
- CenterActionButton: opens "Find Nearby" modal or triggers location
- Tab bar: Home (active), Settings
- Safe area handling (notch, home indicator)

**Tokens:**
- Map: `surface.mapTerrain`
- Floating cards: `surface.card`
- Active state: `brand.primary`
- Glow pins: `brand.glow`

### Station List Screen

**Purpose:** Scrollable list of stations  
**Layout:** List with sticky search

**Components:**
- SearchBar (sticky at top)
- StationCard list (each card clickable)
- Skeleton list (while loading)
- EmptyState (no results)

**Key Features:**
- Infinite scroll or pagination
- Tap card → detail screen
- Search filters in real-time

### Station Detail Screen

**Purpose:** Full-screen station information  
**Layout:** Scrollable full-screen card

**Components:**
- Header (thumbnail, name, address, rating)
- SpecRow list (power, chargers, hours)
- ChargerRow list (each charger with status)
- StatusBadge (charger availability)
- ReviewCard list (reviews with ratings)
- CTA buttons (Favorite, Write Review, Share, Call)
- BottomTabBar (for navigation)

**Key Features:**
- Swipe down to dismiss (return to map)
- Favorite button shows state (filled heart = saved)
- Click favorite → adds/removes from favorites
- "Call" button opens phone dialer
- Share opens native share sheet
- Reviews sorted by date (newest first)

**Tokens:**
- Background: `surface.background`
- Cards: `surface.card`
- Headers: `text.main font-bold`

### Search Screen

**Purpose:** Search and filter stations  
**Layout:** Full-screen search input + results list

**Components:**
- SearchBar (large, full focus)
- FilterPills (horizontal scroll)
- StationCard list (results)
- EmptyState (if no results)
- BottomTabBar

**Key Features:**
- Auto-focus search input on screen open
- Real-time filtering as user types
- Clear button on search input
- Results count
- Swipe down on search input to dismiss keyboard

### Favorites Screen

**Purpose:** View and manage saved stations  
**Layout:** List

**Components:**
- StationCard list (each item has favorite heart icon filled)
- EmptyState ("You haven't saved any stations yet")
- "Explore Stations" button in empty state
- BottomTabBar

**Key Features:**
- Tap card → detail screen
- Swipe left on card to show "Remove" action
- Tap "Remove" to unfavorite
- Sort by: date added, rating, distance

### Profile Screen

**Purpose:** User settings and account  
**Layout:** Scrollable form

**Components:**
- Avatar (tap to upload new photo)
- Name Inputs (first + last)
- Email (read-only)
- Phone Input (optional)
- Language Select
- Theme Toggle (light/dark, if applicable)
- Save Button
- Logout Button (danger styling)
- Version Number (bottom)
- BottomTabBar

**Key Features:**
- Avatar upload with camera/gallery options
- Form validation
- Success Toast on save
- Logout warning modal before confirming
- Safe area handling at bottom

### Write Review Screen

**Purpose:** Create/edit a station review  
**Layout:** Full-screen scrollable form

**Components:**
- Station name (header, not editable)
- Star rating selector (1–5, interactive)
- Textarea (comment, optional)
- Character counter (max 500)
- Submit Button
- Cancel Button (or swipe-down)

**Key Features:**
- Star selector: tap star to set rating
- Placeholder text: "What did you think about this station?"
- Form validation: rating required, comment optional
- Success Toast on submit
- Return to detail screen or map after submit
- Loading spinner while submitting

**Tokens:**
- Stars: `status.inUse` (amber/gold color when selected)
- Submit button: `brand.primary`
- Cancel: `text-text-muted`

### Login / Register Screen

**Purpose:** Authentication  
**Layout:** Full-screen form

**Components:**
- Logo (centered at top)
- Email Input
- Password Input
- Sign In Button
- Social Login Buttons (Google, Facebook)
- "Create Account" link
- "Forgot Password" link

**Key Features:**
- Form validation
- Loading spinner on button
- Error Toast on failure
- Keyboard handling (auto-focus, dismiss)
- Social buttons trigger native OAuth flows

**Tokens:**
- Button: `brand.primary`
- Links: `text-brand-primary`

---

## Dashboard App Screens

Data-dense interface for partners and admins.

### Dashboard Home Screen

**Purpose:** Overview of platform metrics (Admin only)  
**Layout:** Grid of StatCard components + charts

**Components:**
- AppShell (sidebar + topbar + content)
- StatCard list (KPIs: users, stations, chargers, events)
- Chart (daily active users, stations added, chargers used)
- Recent activity list (latest reviews, new stations, etc.)
- Quick links to management screens

**Key Features:**
- StatCard shows metric, trend (up/down), and percentage change
- Date range selector (today, week, month, year)
- Chart updates based on date range
- Click metric card → filtered detail screen

### Partner Dashboard Screen

**Purpose:** Overview for a single partner  
**Layout:** Grid of StatCard + tables

**Components:**
- AppShell
- StatCard list (partner-specific KPIs: own stations, chargers, ratings)
- Stations table (list of this partner's stations)
- Recent reviews table (for this partner's stations)
- Quick action button: "Add Station"

**Key Features:**
- Only shows this partner's data
- Partner_id enforced by JWT in middleware
- Click station row → detail screen
- Click review row → moderation modal

### Stations Management Screen

**Purpose:** Admin: manage all stations; Partner: manage own stations  
**Layout:** DataTable with filters

**Components:**
- AppShell
- Filter section (by partner, city, status)
- DataTable (sortable, filterable, paginated)
  - Columns: Name, Partner, City, Chargers, Rating, Status, Actions
- Action buttons: Edit, Delete, View Details
- "Add Station" button (admin only)
- Bulk actions (delete selected, change status)

**Key Features:**
- Click row → detail modal
- Click "Edit" → edit form modal
- Click "Delete" → confirmation modal
- Status column shows: Active, Inactive, Archived
- Click "View Details" → drill into station detail

**Tokens:**
- Table header: `bg-brand-sageLight`
- Active status: `status.available`
- Inactive status: `status.maintenance`

### Station Detail Screen

**Purpose:** Edit a single station  
**Layout:** Form in modal or dedicated screen

**Components:**
- AppShell
- Form section: Name, Address, City, Amenities, Operating Hours
- Location map (preview)
- Chargers list (sortable, edit/delete each)
- "Add Charger" button
- Save Button
- Cancel Button
- Delete Button (danger styling, admin only)

**Key Features:**
- Validate required fields
- Auto-save draft (optional, in localStorage)
- Success Toast on save
- Charger inline editor (click to edit)
- Drag-to-reorder chargers
- Geolocation helper (auto-fill lat/lng from address)

### Chargers Management Screen

**Purpose:** View and edit chargers across all stations  
**Layout:** DataTable with grouping

**Components:**
- AppShell
- Filter section (by station, connector type, status)
- DataTable
  - Columns: Connector Type, Power Output, Station, Status, Last Updated, Actions
  - Grouped by station (collapsible)
- Action buttons: Edit, Delete
- "Add Charger" button
- Bulk update status button

**Key Features:**
- Inline status editor (click status → dropdown to update)
- Last Updated shows timestamp
- Click row → edit modal
- Group by station for overview

### Partners Management Screen

**Purpose:** Admin only: manage all partners  
**Layout:** DataTable with actions

**Components:**
- AppShell
- DataTable
  - Columns: Name, Contact, Email, Stations, Created, Actions
- Action buttons: Edit, Delete, View Details
- "Add Partner" button
- Filter by status (active, inactive)

**Key Features:**
- Click name → partner detail screen
- Click "Edit" → edit modal
- Soft delete (archive) vs hard delete
- Success Toast on create/update

### Partner Detail Screen

**Purpose:** Admin: edit a single partner  
**Layout:** Form in modal or page

**Components:**
- Form section: Name, Contact, Email, Phone, Address, City
- Related stations table (this partner's stations)
- Related users table (this partner's team members)
- Save Button
- Delete Button (danger, admin only)

**Key Features:**
- Validate all fields
- Charger table shows station overview
- Users table shows team member list
- Add/remove users from partner

### Reviews Moderation Screen

**Purpose:** Admin: moderate all reviews  
**Layout:** DataTable with filtering

**Components:**
- AppShell
- Filter section (by status: pending, approved, rejected; by station)
- DataTable
  - Columns: Author, Station, Rating, Comment, Status, Actions
- Action buttons: Approve, Reject, Delete
- Bulk actions: approve all, reject all
- Detail modal for reviewing comment

**Key Features:**
- Status badge (pending, approved, rejected)
- Click row → review detail modal
- Approve button → changes status, sends notification
- Reject button → removes review, optional reason email
- Delete button → hard delete (no recovery)

### Reports & Analytics Screen

**Purpose:** Admin: view platform analytics  
**Layout:** Grid of charts and tables

**Components:**
- AppShell
- Date range selector (date picker)
- Charts:
  - Daily active users (line chart)
  - Stations added over time (bar chart)
  - Chargers by status (pie chart)
  - Top rated stations (bar chart)
  - Most reviewed stations (table)
- Export button (CSV, PDF)

**Key Features:**
- Drill down: click chart element → filtered detail view
- Date range affects all charts
- Export generates file download
- Real-time or cached aggregates (depending on scale)

**Tokens:**
- Charts: use brand and status colors for series
- Export button: `brand.primary`

---

## Screen Flow Diagram

```
DRIVER WEB & MOBILE
├─ Home / Map
│  ├─ → Station Detail
│  │  ├─ → Write Review (login modal if needed)
│  │  └─ → Share (native dialog)
│  ├─ → Search Results
│  └─ → Favorites
├─ Search
│  └─ → Station Detail
├─ Favorites
│  └─ → Station Detail
├─ Profile
│  └─ → Logout (back to Home)
└─ Login / Register
   └─ → Home (on success)

DASHBOARD (Admin view)
├─ Dashboard Home
│  ├─ → Stations Management
│  ├─ → Partners Management
│  ├─ → Reviews Moderation
│  └─ → Reports & Analytics
├─ Stations Management
│  └─ → Station Detail (edit)
├─ Chargers Management
│  └─ → Charger Detail (edit)
├─ Partners Management
│  └─ → Partner Detail
├─ Reviews Moderation
│  └─ → Review Detail (modal)
└─ Reports & Analytics
   └─ → (drill-down to detail tables)

DASHBOARD (Partner view)
├─ Partner Dashboard
│  ├─ → Stations Management (partner's stations only)
│  └─ → Recent Reviews (partner's reviews only)
├─ Stations Management
│  └─ → Station Detail
└─ Profile
   └─ → Logout (back to home)
```

---

## Responsive Behavior

### Driver Web App
- **Desktop (>1024px):** Full-bleed map, right sidebar with station card
- **Tablet (768–1024px):** Full-bleed map, bottom sheet for station card
- **Mobile (<768px):** Same as Driver Mobile App (bottom sheet pattern)

### Dashboard App
- **Desktop (>1024px):** Sidebar always visible, content adjusts width
- **Tablet (768–1024px):** Collapsible sidebar, content expands
- **Mobile (<768px):** Sidebar in hamburger menu, full-width content

### Mobile Safe Areas
- Top inset: handled by MobileTopBar (iOS notch, Android status bar)
- Bottom inset: handled by BottomTabBar (iOS home indicator, Android nav bar)

---

## Accessibility Requirements

All screens must meet WCAG 2.1 AA minimum:

- ✅ Keyboard navigation (Tab, Enter, Escape, Arrow keys)
- ✅ Screen reader support (semantic HTML, ARIA labels)
- ✅ Color contrast (see design-tokens.md)
- ✅ Focus indicators (visible, not hidden)
- ✅ Error messages (text + visual, color not only)
- ✅ Touch target size (minimum 44×44px on mobile)
- ✅ Alternative text for images
- ✅ Skip links on web apps (skip to main content)

---

## RTL Testing Screens

When testing Arabic RTL:

1. Home / Map → SearchBar, FilterPills, ZoomControls
2. Station Detail → SpecRow (labels right, values left)
3. Search Results → StationCard (image right, text left)
4. Dashboard Stations → Table (column order reversed)
5. Profile → Form inputs (labels right, inputs left)

---

**Document Version:** 1.0  
**Last Updated:** 2026-06-05  
**Status:** Complete with Driver theme specifications
