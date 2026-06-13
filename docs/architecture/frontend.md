# Frontend Architecture

## Version: 1.0
## Status: Active
## Core Philosophy: Documentation is the system. Code is just its execution.

---

## 📱 OVERVIEW

BorneMap frontend consists of three applications plus shared packages. All frontend code follows strict rules to ensure consistency, performance, and maintainability.

---

## 🏗️ APPLICATION STRUCTURE

```
source/front/
├── apps/
│   ├── mobile-driver/      # React Native (Expo SDK 54)
│   ├── web-driver/         # React (Web)
│   └── dashboard/          # React (Admin Dashboard - MVP-2+)
└── packages/
    ├── @bm/types/          # TypeScript definitions
    ├── @bm/api-client/     # API communication layer
    ├── @bm/utils/          # Utility functions
    └── @bm/design-tokens/  # Design system
```

---

## 📲 MOBILE DRIVER

**Purpose:** Driver mobile application for station discovery

**Tech Stack:**
- React Native
- Expo SDK 54
- @bm/api-client
- @bm/design-tokens
- React Query (server state)

**Features (MVP-1 only):**
- Station discovery via map
- Nearby station search
- Station details view
- Basic analytics events

**Constraints:**
- Mobile-first design
- Touch interactions only
- Battery optimization required
- No desktop gestures

**Architecture Rules:**
- All API calls through @bm/api-client
- No fetch() or axios usage
- Map rendering through MapContainer abstraction
- Loading and error states required
- No direct database access

**Directory Structure:**
```
mobile-driver/
├── src/
│   ├── components/
│   ├── screens/
│   ├── hooks/
│   ├── services/
│   ├── stores/            # UI state (Zustand)
│   ├── utils/             # Business logic
│   └── types/             # @bm/types imports
├── package.json
└── app.json
```

---

## 🌐 WEB DRIVER

**Purpose:** Web driver application for station discovery

**Tech Stack:**
- React
- React Router
- @bm/api-client
- @bm/design-tokens
- React Query (server state)
- Leaflet (via MapContainer)

**Features (MVP-1 only):**
- Station discovery via map
- Nearby station search
- Station details view
- Basic analytics events

**Constraints:**
- Responsive design
- Desktop and tablet optimized
- Keyboard interactions supported
- Battery optimization (web)

**Architecture Rules:**
- All API calls through @bm/api-client
- No fetch() or axios usage
- Map rendering through MapContainer abstraction
- Loading and error states required
- No direct database access

**Directory Structure:**
```
web-driver/
├── src/
│   ├── components/
│   ├── pages/
│   ├── hooks/
│   ├── services/
│   ├── stores/            # UI state (Zustand)
│   ├── utils/             # Business logic
│   └── types/             # @bm/types imports
├── package.json
└── tsconfig.json
```

---

## 🖥️ DASHBOARD

**Purpose:** Admin dashboard for station and user management

**Tech Stack:**
- React
- shadcn/ui
- @bm/api-client
- @bm/design-tokens
- React Query (server state)
- Recharts (for analytics)

**Features (MVP-2+ only):**
- Station management (CRUD)
- User management
- Partner management
- Analytics and reporting
- Operational workflows

**Constraints:**
- Admin-only access
- All features authenticated
- Comprehensive error handling
- Performance monitoring

**Architecture Rules:**
- All API calls through @bm/api-client
- No fetch() or axios usage
- Strict authorization checks
- Loading and error states required
- No direct database access

**Directory Structure:**
```
dashboard/
├── src/
│   ├── components/
│   ├── pages/
│   ├── hooks/
│   ├── services/
│   ├── stores/            # UI state (Zustand)
│   ├── utils/             # Business logic
│   └── types/             # @bm/types imports
├── package.json
└── tsconfig.json
```

---

## 📦 SHARED PACKAGES

### @bm/types

**Purpose:** Central type definitions

**Contents:**
- Station interfaces
- User interfaces
- API request/response types
- Error types
- Analytics event types

**Usage:**
- All frontend and backend code
- TypeScript only (no runtime code)
- Single source of truth

**Constraints:**
- No runtime logic
- No external dependencies
- Version controlled and imported

### @bm/api-client

**Purpose:** API communication layer

**Contents:**
- Request handlers
- Response parsers
- Error handling
- Type validation
- Authentication integration

**Usage:**
- All frontend apps
- All API calls
- No fetch() or axios usage

**Constraints:**
- No external API calls
- All @bm/types used
- Follows API contract rules
- Validates responses

### @bm/utils

**Purpose:** Utility functions

**Contents:**
- Date formatting
- Number formatting
- String manipulation
- Validation functions
- Business logic utilities

**Usage:**
- All frontend and backend code
- Reusable logic

**Constraints:**
- No UI-specific code
- No API calls
- Pure functions preferred

### @bm/design-tokens

**Purpose:** Design system

**Contents:**
- Colors (primary, secondary, etc.)
- Spacing scales
- Typography scales
- Border radii
- Shadows

**Usage:**
- All frontend UI
- Theme providers
- Component styling

**Constraints:**
- No runtime logic
- CSS variables preferred
- Theme-aware

---

## 🧠 STATE MANAGEMENT

### Server State → React Query
- All API data fetching
- Server state caching
- Automatic refetching
- Error handling

### UI State → Local or Zustand
- User interactions
- UI modal states
- Temporary UI state
- Form state

**Rules:**
- No shared global state across apps
- Each app manages its own UI state
- State isolation prevents conflicts

---

## 🎨 UI COMPONENTS

### Design System
- Uses @bm/design-tokens
- Consistent across all apps
- Theme support
- Dark mode optional

### Map Components
- All map rendering through MapContainer abstraction
- No direct map library usage in apps
- MapContainer handles platform-specific implementations

### Error Handling
- Loading states required (skeleton preferred)
- Empty states required
- Error states required
- Retry options for failed requests

---

## 📱 MOBILE RULES

### Touch Interactions
- Touch event handling
- Gesture recognizers
- Touch feedback
- Mobile performance optimization

### Battery Optimization
- Map rendering optimized
- Background location throttled
- Network requests efficient
- Code splitting required

### App Structure
- React Native Navigation
- Expo Configuration
- Native modules (where needed)
- Platform-specific code

---

## 🌐 WEB RULES

### Responsive Design
- Mobile-first approach
- Breakpoints defined
- Touch targets optimized
- Desktop features supported

### Performance
- Code splitting
- Lazy loading
- Optimized bundle sizes
- Efficient rendering

### Accessibility
- Keyboard navigation
- Screen reader support
- Focus management
- Semantic HTML

---

## 🔐 SECURITY RULES

### Authentication
- All features require authentication (except MVP-1 station discovery)
- JWT tokens validated
- Authorization checks on protected endpoints
- Session management proper

### Data Security
- No sensitive data in local storage
- Tokens encrypted in memory
- No hardcoded secrets
- Secure HTTP only cookies

### Input Validation
- All user input validated
- SQL injection prevention
- XSS prevention
- CSRF protection

---

## 🧪 TESTING RULES

### Testing Framework
- Unit tests for components
- Integration tests for flows
- E2E tests for critical paths
- API client tests

### Coverage Requirements
- Core functionality ≥ 80%
- UI components ≥ 70%
- Utility functions ≥ 90%
- API client ≥ 90%

---

## 🔄 DOCUMENTATION IS SYSTEM

**Frontend architecture rules are documented here.**
**Code must implement documented architecture.**
**Documentation must be updated with changes.**

**Documentation is the system. Code is just its execution.**
