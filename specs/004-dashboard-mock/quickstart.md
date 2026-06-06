# Quickstart Guide: Dashboard App with Mock Data

**Feature**: Dashboard App with Mock Data (Sprint 1.4)
**Date**: 2026-06-06

## Prerequisites

- Node.js 20 or higher
- pnpm 10 or higher
- Git
- Modern web browser (Chrome, Firefox, Safari)

## Installation

1. **Clone the repository** (if not already cloned):
   ```bash
   git clone <repository-url>
   cd BorneMap
   ```

2. **Install dependencies**:
   ```bash
   pnpm install
   ```

3. **Verify the design tokens package is built**:
   ```bash
   cd packages/ui
   pnpm build
   cd ../..
   ```

## Running the Dashboard

1. **Start the development server**:
   ```bash
   cd apps/dashboard
   pnpm dev
   ```

2. **Open the browser**:
   Navigate to `http://localhost:5174` (or the URL shown in terminal output)

3. **Verify the dashboard loads**:
   - You should see the login screen with mock role toggle
   - Click the role toggle to switch between Partner and Admin modes
   - Navigate between screens using the sidebar

## Testing Role-Based Interfaces

### Partner Mode

1. Click the role toggle to select "Partner"
2. The sidebar should show:
   - Overview
   - My Stations
   - Station Edit
   - Charger Management
   - Availability Update
   - Reports

3. Verify the Overview screen displays:
   - 4 StatCards (stations, chargers, reviews, availability)
   - DataCard listing partner's stations

### Admin Mode

1. Click the role toggle to select "Admin"
2. The sidebar should show:
   - Overview
   - Users
   - Partners
   - Stations
   - Chargers
   - Reviews
   - Reports

3. Verify the Overview screen displays:
   - 6 StatCards (users, partners, stations, chargers, reviews, events)
   - DataCard with live station list
   - DataCard with active drivers table

## Testing RTL Support

1. Open browser DevTools (F12)
2. Navigate to the "Application" or "More Tools" → "Language" settings
3. Change language to Arabic (ar)
4. Reload the dashboard
5. Verify RTL layout:
   - Sidebar aligns to the right
   - Tables display correctly in RTL
   - Form elements align correctly for RTL

6. Switch back to French (fr)
7. Verify LTR layout is restored

## Project Structure

```
apps/dashboard/
├── src/
│   ├── components/           # Dashboard-specific components
│   │   ├── AppShell/         # Layout wrapper (Sidebar + TopBar)
│   │   ├── PageContent/      # Scrollable content area
│   │   ├── DataCard/         # Panel with header and body
│   │   ├── DataTable/        # Sortable, paginated table
│   │   └── StatCard/         # Metric display card
│   ├── screens/              # Screen components (13 screens)
│   ├── mocks/                # Mock data files
│   │   ├── partners.ts
│   │   ├── stations.ts
│   │   ├── chargers.ts
│   │   ├── users.ts
│   │   ├── reviews.ts
│   │   └── reports.ts
│   ├── i18n/                 # Internationalization
│   │   ├── ar.json           # Arabic translations
│   │   ├── fr.json           # French translations
│   │   └── index.ts          # i18next configuration
│   ├── hooks/                # Custom React hooks
│   │   ├── useRole.ts        # Role state management
│   │   ├── useMockData.ts    # Mock data access
│   │   └── useNavigation.ts  # Navigation utilities
│   ├── context/              # React Context providers
│   │   └── RoleContext.tsx   # Role state context
│   ├── types/                # TypeScript interfaces
│   │   └── index.ts
│   ├── App.tsx               # Root component
│   └── index.css             # Global styles
├── package.json              # Dependencies and scripts
├── tsconfig.json             # TypeScript configuration
├── vite.config.ts            # Vite configuration
├── tailwind.config.js        # Tailwind configuration
└── app.json                  # App metadata
```

## Available Scripts

```bash
# Development
pnpm dev              # Start development server
pnpm build            # Build for production
pnpm preview          # Preview production build

# Code quality
pnpm lint             # Run ESLint
pnpm typecheck        # Run TypeScript type checking
```

## Design Tokens

All visual values (colors, spacing, typography, shadows, radius) are consumed from the `packages/ui` design tokens package.

**Do not hardcode visual values** in the dashboard app.

To use tokens:
```typescript
import { colors, spacing, typography } from '@borne-map/ui';

// Example
const buttonStyle = {
  backgroundColor: colors.brand.primary,
  padding: spacing.md,
  fontSize: typography.base,
};
```

## Mock Data

All data is sourced from local TypeScript files in `src/mocks/`. No backend calls are made in this phase.

To access mock data:
```typescript
import { mockStations, getStationsByPartner } from '../mocks/stations';

const partnerStations = getStationsByPartner(partnerId);
```

## Role Management

Role state is managed via React Context in `src/context/RoleContext.tsx`.

To access current role:
```typescript
import { useRole } from '../hooks/useRole';

const { role, setRole } = useRole();
```

To switch roles:
```typescript
setRole('admin');  // or 'partner'
```

## Internationalization

The dashboard supports Arabic (RTL) and French languages.

To use translations:
```typescript
import { useTranslation } from 'react-i18next';

const { t, i18n } = useTranslation();
const label = t('dashboard.overview.stations');
```

To change language:
```typescript
i18n.changeLanguage('ar');  // or 'fr'
```

## Troubleshooting

### Dashboard doesn't load

- Ensure `packages/ui` is built: `cd packages/ui && pnpm build && cd ../..`
- Check that all dependencies are installed: `pnpm install`
- Verify the port is not in use (default: 5174)

### RTL layout issues

- Open browser DevTools and verify the `dir` attribute on the `<html>` element is set to `rtl` for Arabic
- Check that Tailwind RTL modifiers are used (e.g., `rtl:ml-4` instead of `ml-4`)
- Verify sidebar alignment changes when switching languages

### Role toggle not visible

- The role toggle is a dev-only feature and should be visible in the corner of the screen
- Check that `RoleContext` is wrapping the application in `App.tsx`
- Verify `useRole()` hook is imported correctly

### Design tokens not resolving

- Ensure `@borne-map/ui` is listed in dependencies: `pnpm list @borne-map/ui`
- Check that the tokens are imported correctly: `import { colors } from '@borne-map/ui'`
- Verify Tailwind config extends the base tokens: see `tailwind.config.js`

## Next Steps

After verifying the dashboard runs correctly:

1. **Review all screens**: Ensure all 13 screens display with mock data
2. **Test RTL layout**: Switch to Arabic and verify all screens align correctly
3. **Test role switching**: Toggle between Partner and Admin modes
4. **Verify navigation**: Navigate between all screens using the sidebar
5. **Check TypeScript compilation**: Run `pnpm typecheck` to ensure zero errors

## Additional Resources

- [Sprint Specification](./spec.md)
- [Implementation Plan](./plan.md)
- [Data Model](./data-model.md)
- [Mock Data Contracts](./contracts/README.md)
- [Constitution](../../.specify/memory/constitution.md)
- [Project Implementation Plan](../../docs/core/implementation-plan.md)