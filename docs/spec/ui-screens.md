# UI Screen Inventory

---

## Mobile Driver App (Expo SDK 54)

### Screen: MapView (default / landing)

| Aspect | Detail |
|--------|--------|
| **Purpose** | Primary interface — discover charging stations on a map |
| **Route** | `/` (tab: Map) |
| **Access** | Public (no auth required) |
| **Inputs** | Device GPS location (optional), map pan/zoom, tap on marker |
| **Key UI** | Full-screen map (`react-native-maps`), station markers with type icon, clustering at zoom < 13 |
| **Outputs** | Station markers rendered on map; tap marker -> open bottom sheet with station summary |
| **States** | Loading (spinner over map), empty (map centered on Tunisia with "Zoom in to see stations" message), error (banner with retry button) |
| **Data** | `GET /api/v1/nearby` with current map center coords |

### Screen: StationDetail (bottom sheet / full screen)

| Aspect | Detail |
|--------|--------|
| **Purpose** | Show station information and charger list |
| **Route** | `/stations/:id` (presented as bottom sheet on map screen) |
| **Access** | Public |
| **Inputs** | Station ID from marker tap |
| **Key UI** | Bottom sheet (draggable), station photo header, charger list with type/connector/power/status, action buttons |
| **Outputs** | Station name, address, distance, opening hours, charger list, "Add to favorites" button (if logged in) |
| **States** | Loading (skeleton), loaded, error (inline message) |
| **Data** | `GET /api/v1/stations/:id` |

### Screen: Favorites (tab)

| Aspect | Detail |
|--------|--------|
| **Purpose** | View saved favorite stations |
| **Route** | `/favorites` (tab: Favorites) |
| **Access** | Driver auth required |
| **Inputs** | None |
| **Key UI** | List of favorited stations, tap to navigate to station detail, swipe to remove |
| **Outputs** | List view with station name, address, distance |
| **States** | Loading, empty ("No favorites yet"), list |
| **Data** | `GET /api/v1/favorites` |

### Screen: Login

| Aspect | Detail |
|--------|--------|
| **Purpose** | Driver login / registration |
| **Route** | `/login` (modal) |
| **Access** | Public (unauthenticated users) |
| **Inputs** | Email, password, or social login buttons |
| **Key UI** | Email/password form, "Sign in with Google", "Sign in with Facebook", link to register |
| **Outputs** | JWT tokens stored in secure storage |
| **States** | Idle, submitting, error (inline message per field) |
| **Data** | `POST /api/v1/auth/login`, `POST /api/v1/auth/social-login` |

### Screen: Register

| Aspect | Detail |
|--------|--------|
| **Purpose** | New driver account creation |
| **Route** | `/register` (modal) |
| **Access** | Public |
| **Inputs** | Display name, email, password, confirm password |
| **Key UI** | Registration form with password strength indicator |
| **Outputs** | Success message, redirect to login |
| **States** | Idle, submitting, success, error |
| **Data** | `POST /api/v1/auth/register` |

### Screen: Profile (tab)

| Aspect | Detail |
|--------|--------|
| **Purpose** | View and edit driver profile |
| **Route** | `/profile` (tab: Profile) |
| **Access** | Driver auth required |
| **Inputs** | Display name (editable) |
| **Key UI** | Profile info, settings links, logout button |
| **Outputs** | Updated display name |
| **States** | Loading, loaded, saving |
| **Data** | `GET /api/v1/users/me` (load), `PATCH /api/v1/users/me` (update display_name) |

### Screen: NotFound

| Aspect | Detail |
|--------|--------|
| **Purpose** | Catch-all for invalid routes |
| **Route** | `*` |
| **Access** | Public |
| **Key UI** | "Page not found" with link back to map |

---

## Web Driver App (React + Leaflet)

Mirrors mobile driver with same screens; platform-idiomatic differences noted.

### Screen List

| Screen | Route | Notes |
|--------|-------|-------|
| MapView | `/` (default) | Full-viewport map; station list sidebar as alternative layout |
| StationDetail | `/stations/:id` | Side panel (slides in from right) instead of bottom sheet |
| Favorites | `/favorites` | Sidebar tab with icon-button remove |
| Login | `/login` | Centered modal overlay |
| Register | `/register` | Centered modal overlay |
| Profile | `/profile` | Top-right dropdown accessible from nav bar |
| NotFound | `*` | Centered message with link home |

### Layout Differences

- Map fills viewport; sidebar overlays on left (collapsible)
- Station list view available as sidebar tab (alternative to map browsing)
- Desktop-optimized: multi-pane layout (map + list + detail)

---

## Dashboard App (React + shadcn/ui)

### Screen: Login

| Aspect | Detail |
|--------|--------|
| **Purpose** | Admin / Partner login |
| **Route** | `/login` |
| **Access** | Public |
| **Inputs** | Email, password |
| **Key UI** | Centered login card, branded |
| **Outputs** | JWT tokens, redirect to dashboard |
| **States** | Idle, submitting, error |
| **Data** | Direct to Keycloak OIDC `/auth/realms/borne-map/protocol/openid-connect/token` |

### Screen: DashboardHome

| Aspect | Detail |
|--------|--------|
| **Purpose** | Overview stats and quick actions |
| **Route** | `/` |
| **Access** | Admin or Partner auth |
| **Inputs** | None |
| **Key UI** | Stats cards (total stations, active chargers, etc.), recent activity list |
| **Outputs** | Aggregated data display |
| **States** | Loading (skeleton cards), loaded, empty ("No data yet") |
| **Data** | `GET /api/v1/admin/analytics/overview` |

### Screen: StationsList

| Aspect | Detail |
|--------|--------|
| **Purpose** | Manage all stations (admin) or own stations (partner) |
| **Route** | `/stations` |
| **Access** | Admin or Partner auth |
| **Inputs** | Search, filter (status, city), pagination |
| **Key UI** | Data table (sortable, filterable, paginated), action buttons per row |
| **Outputs** | Table with station name, city, status, charger count, actions |
| **States** | Loading, empty ("No stations"), error |
| **Data** | Admin: `GET /api/v1/admin/stations`; Partner: `GET /api/v1/partner/stations` |

### Screen: StationDetail

| Aspect | Detail |
|--------|--------|
| **Purpose** | View a single station with its chargers before editing |
| **Route** | `/stations/:id` |
| **Access** | Admin or Partner auth; partner sees own stations only |
| **Inputs** | Station ID from list row click |
| **Key UI** | Station info card, charger list/table, action buttons (edit, delete) |
| **Outputs** | Full station details + attached chargers |
| **States** | Loading, loaded, not found, error |
| **Data** | Admin: `GET /api/v1/admin/stations/:id`; Partner: `GET /api/v1/partner/stations/:id` |

### Screen: StationForm (create / edit)

| Aspect | Detail |
|--------|--------|
| **Purpose** | Create or edit a station |
| **Route** | `/stations/new`, `/stations/:id/edit` |
| **Access** | Admin or Partner auth; ownership verified on edit |
| **Inputs** | All station fields + map picker for location |
| **Key UI** | Form with validation, map pin selector, charger sub-form (modal dialog — add/edit/delete chargers inline without navigating away) |
| **Outputs** | Success -> redirect to station detail / station list |
| **States** | Loading (edit mode), submitting, validation errors, success |

### Screen: PartnerList

| Aspect | Detail |
|--------|--------|
| **Purpose** | Manage partners (admin only) |
| **Route** | `/partners` |
| **Access** | Admin auth only |
| **Inputs** | Search, filter |
| **Key UI** | Data table, invite button |
| **Outputs** | Partner list with status, station count, actions |

### Screen: PartnerDetail

| Aspect | Detail |
|--------|--------|
| **Purpose** | View a single partner profile before editing |
| **Route** | `/partners/:id` |
| **Access** | Admin auth only |
| **Inputs** | Partner ID from list row click |
| **Key UI** | Partner info card, status badge, station count, action buttons (edit, suspend, approve/reject if pending) |
| **Outputs** | Full partner details + linked stations |
| **States** | Loading, loaded, not found, error |
| **Data** | `GET /api/v1/admin/partners/:id`, `GET /api/v1/admin/partners/:id/stations` |

### Screen: PartnerForm (create / edit)

| Aspect | Detail |
|--------|--------|
| **Purpose** | Create or edit partner (admin invite) |
| **Route** | `/partners/new`, `/partners/:id/edit` |
| **Access** | Admin auth |
| **Inputs** | Name, email, type, phone |
| **Outputs** | Success -> partner created + invite sent |

### Screen: PendingApprovals

| Aspect | Detail |
|--------|--------|
| **Purpose** | Review partner self-registrations (admin only) |
| **Route** | `/approvals` |
| **Access** | Admin auth |
| **Inputs** | Approve/reject actions |
| **Key UI** | List of pending partners with details, approve/reject buttons |
| **States** | Empty ("No pending approvals"), list |

### Screen: AnalyticsDashboard

| Aspect | Detail |
|--------|--------|
| **Purpose** | Charts and metrics over time (MVP-5) |
| **Route** | `/analytics` |
| **Access** | Admin auth only |
| **Inputs** | Date range, metric selector |
| **Key UI** | Line/bar charts (recharts), date range picker, export button |
| **Outputs** | Session counts, energy dispensed, revenue, utilization rates |
| **States** | Loading (chart skeletons), loaded, empty ("No data for selected period"), error |
| **Data** | `GET /api/v1/admin/analytics/sessions`, `GET /api/v1/admin/analytics/energy`, `GET /api/v1/admin/analytics/revenue` |

### Screen: Settings

| Aspect | Detail |
|--------|--------|
| **Purpose** | User profile and preferences |
| **Route** | `/settings` |
| **Access** | Any authenticated user |
| **Inputs** | Display name |
| **Key UI** | Profile form, "Change password" link -> Keycloak account page (external redirect), logout button |

### Screen: Unauthorized (401)

| Aspect | Detail |
|--------|--------|
| **Purpose** | Inform user they must log in |
| **Route** | redirect from guarded routes |
| **Access** | Public (no token) |
| **Key UI** | "Please sign in to access this page" with login button |

### Screen: Forbidden (403)

| Aspect | Detail |
|--------|--------|
| **Purpose** | Inform user they lack permissions |
| **Route** | redirect from admin routes for partner users |
| **Access** | Authenticated but wrong role |
| **Key UI** | "You don't have permission to access this page" with contact-admin message |

### Screen: NotFound (404)

| Aspect | Detail |
|--------|--------|
| **Purpose** | Catch-all for invalid routes |
| **Route** | `*` |
| **Access** | Public |
| **Key UI** | "Page not found" with link to dashboard home |
