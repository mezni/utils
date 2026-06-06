# Research: Dashboard App with Mock Data

**Feature**: Dashboard App with Mock Data (Sprint 1.4)
**Date**: 2026-06-06

## Technology Decisions

### Decision 1: UI Framework and Tooling Stack

**Selected**: Vite 6 + React 19 + TypeScript 5.7 + Tailwind CSS 4

**Rationale**:
- Consistency with Sprint 1.2 (Driver Web App) which uses the same stack
- Vite provides fast development server and optimized builds
- React 19 is the latest stable version with improved performance and developer experience
- TypeScript ensures type safety across the codebase, especially for mock data structures
- Tailwind CSS 4 is the latest version with improved performance and new features
- All three apps (driver-web, driver-mobile, dashboard) share the same TypeScript configuration and build tools

**Alternatives Considered**:
- **Next.js 14**: Too complex for this phase, adds server-side routing that's not needed for mock data phase
- **Create React App (CRA)**: Deprecated and slower than Vite
- **Vue.js 3**: Different from the React-based driver-web app, would increase learning curve and inconsistency

---

### Decision 2: Routing Strategy

**Selected**: react-router-dom 7 with role-based route guards

**Rationale**:
- Industry-standard routing library for React applications
- Supports client-side routing without page reloads (FR-014 requirement)
- Route guards can be implemented to hide/disable navigation based on user role (FR-015)
- Compatible with Vite and works well with React 19
- Allows for programmatic navigation when switching roles (FR-016)

**Alternatives Considered**:
- **TanStack Router**: More powerful but adds complexity for this phase's requirements
- **React Router v6**: Previous version, upgrading to v7 provides better TypeScript support

---

### Decision 3: Role Management and Mock Authentication

**Selected**: React Context for role state with dev-only toggle UI

**Rationale**:
- React Context provides a simple way to share role state across the application without complex state management
- Dev-only toggle allows easy testing of both partner and admin interfaces during development (FR-014 requirement)
- Context is scoped to the dashboard app and will be replaced by Keycloak in Phase 4
- Meets requirement for role switching within 1 second (SC-004)

**Alternatives Considered**:
- **Redux**: Overkill for simple role state management
- **URL parameter**: Would expose role state in URL, not suitable for mock authentication
- **localStorage**: Would persist role across page reloads, not ideal for dev-only toggle

---

### Decision 4: Internationalization (i18n) and RTL Support

**Selected**: react-i18next with i18next backend and browser language detection

**Rationale**:
- Proven solution used in Sprint 1.2 (Driver Web App) for Arabic and French support
- React-i18next provides hooks and components for translation
- Browser language detection automatically selects Arabic or French based on user preferences
- RTL support is handled via CSS and React Context, not by i18n library itself (constitution requirement)
- Ensures Arabic RTL layout works correctly (FR-010 through FR-013 requirements)

**Alternatives Considered**:
- **FormatJS (React Intl)**: Good alternative but less widely used in the project ecosystem
- **Custom solution**: Would require building translation keys management and RTL handling from scratch

---

### Decision 5: Component Library Integration

**Selected**: Consume all visual values from `packages/ui` design tokens

**Rationale**:
- Constitutional requirement (Principle VIII) - all visual values must be from tokens
- Ensures visual consistency across web, mobile, and dashboard applications
- Tokens package is already established from Sprint 1.1
- Reduces code duplication and ensures single source of truth for design (FR-019 requirement)
- Tailwind config extends tokens from packages/ui/tailwind.config.base.js

**Alternatives Considered**:
- **Hardcoded values**: Violates constitution and creates maintenance burden
- **Separate dashboard tokens**: Would create inconsistency across applications

---

### Decision 6: Mock Data Structure

**Selected**: TypeScript files in `src/mocks/` with typed interfaces

**Rationale**:
- Reuse same 15 stations, 50+ chargers, and 60+ reviews from driver apps (assumption from spec)
- TypeScript provides compile-time type checking for mock data structures
- Easy to maintain and extend as data requirements grow
- Static files can be imported directly without build-time processing
- Aligns with data model defined in spec (Partner, Station, Charger, User, Review, Report entities)

**Alternatives Considered**:
- **JSON files**: No type safety at compile time, harder to maintain
- **Mock Service Worker (MSW)**: Overkill for static mock data, adds unnecessary complexity

---

### Decision 7: Table and Data Visualization Components

**Selected**: Custom DataTable component with sorting and pagination, StatCard for metrics

**Rationale**:
- Custom components provide exact control over styling and behavior
- DataTable implements sorting and pagination as required (FR-007)
- StatCard displays metrics with optional trend indicators
- Chart placeholders are simple gray rectangles (assumption from spec), not actual chart libraries
- Components can be styled using design tokens from packages/ui

**Alternatives Considered**:
- **React Table v8**: Powerful but complex for current requirements
- **Material-UI Table**: Tied to Material-UI design system, conflicts with our tokens
- **TanStack Table**: Advanced features not needed for this phase

---

### Decision 8: Typography

**Selected**: Inter font for dashboard application

**Rationale**:
- Constitutional requirement (Section VII) specifies Inter for dashboard apps
- Inter is optimized for UI design and provides excellent readability
- Different from Plus Jakarta Sans used in web and mobile apps, making dashboard visually distinct
- Loaded via Google Fonts with support for Arabic and Latin characters

**Alternatives Considered**:
- **Plus Jakarta Sans**: Used in driver apps, but constitution specifies Inter for dashboard
- **Roboto**: Good alternative but Inter is the constitutionally mandated choice

---

## Best Practices

### State Management

- Use React Context for role state (useRole.ts)
- Use React hooks for local component state (useState, useEffect)
- Avoid global state management libraries for this phase - not needed

### Component Architecture

- Each component in its own directory with index.ts export
- TypeScript interfaces for component props
- Use slots pattern for DataCard and AppShell (CardHeader, body slot)
- Separate concerns: presentation vs. logic

### Routing and Navigation

- Use nested routes in react-router-dom for role-based layouts
- Route guards to hide/disable navigation items based on role
- Implement redirect when switching roles to a screen that doesn't exist
- Use role-aware routing in App.tsx

### RTL Support

- Use CSS logical properties (margin-inline-start, padding-inline-end) instead of directional properties
- React Context for RTL state (isRTL)
- Tailwind config includes RTL modifiers (rtl:ml-4 vs ltr:ml-4)
- Test RTL layout using browser dev tools language switching

### Mock Data Management

- Centralize mock data in src/mocks/ directory
- Use TypeScript interfaces to enforce type safety
- Create useMockData.ts hook to provide data to components
- Reuse data structures from driver apps (stations, chargers, reviews)

### Testing

- Use Vitest for component tests
- Manual testing for RTL layout using browser dev tools
- Verify all screens display correctly with mock data
- Test role switching functionality

### Accessibility

- Use semantic HTML elements
- Provide ARIA labels where necessary
- Ensure keyboard navigation works for all interactive elements
- Test color contrast against WCAG 2.1 AA standard
- Status colors (green/amber/red) must have non-color indicators (dot + text label)

### Build and Deployment

- Use Vite for development server and production builds
- Follow pnpm monorepo conventions
- No external build scripts or CI/CD (constitutional constraint)
- Manual deployment following documented runbooks

## Dependencies

### Runtime Dependencies

- `react`: ^19.1.0
- `react-dom`: ^19.1.0
- `react-router-dom`: ^7.0.0
- `react-i18next`: ^14.1.0
- `i18next`: ^23.16.0
- `@borne-map/ui`: workspace:* (design tokens and shared components)
- `clsx`: ^2.0.0
- `tailwind-merge`: ^2.0.0

### Development Dependencies

- `@vitejs/plugin-react`: ^4.0.0
- `vite`: ^6.0.0
- `typescript`: ^5.9.3
- `@types/react`: ^19.1.0
- `@types/react-dom`: ^19.1.0
- `vitest`: ^2.0.0
- `eslint`: ^9.0.0
- `tailwindcss`: ^4.0.0

## Risks and Mitigations

### Risk 1: RTL Layout Issues

**Likelihood**: Medium | **Impact**: High (Class A bug if broken)

**Mitigation**:
- Use CSS logical properties extensively
- Test RTL layout on every screen during development
- Follow constitution requirement: RTL failures are Class A bugs
- Use Tailwind's RTL modifiers for directional styles

### Risk 2: Role Switching Edge Cases

**Likelihood**: Medium | **Impact**: Medium

**Mitigation**:
- Implement redirect to Overview when switching to a role where current screen doesn't exist
- Use React Context to manage role state and prevent race conditions
- Test all role switching scenarios in acceptance criteria

### Risk 3: Mock Data Inconsistency

**Likelihood**: Low | **Impact**: Medium

**Mitigation**:
- Reuse same mock data files from driver apps (stations.ts, chargers.ts, reviews.ts)
- Use TypeScript interfaces to enforce type safety
- Document mock data structure in data-model.md

### Risk 4: Performance with Large Mock Data

**Likelihood**: Low | **Impact**: Low

**Mitigation**:
- Use React.memo for expensive components
- Implement virtualization for large tables if needed
- Measure initial load time against 3-second success criteria (SC-006)