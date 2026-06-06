# BorneMap

Electric vehicle charging station management platform.

## Quick Start

### Installation

Due to pnpm supply-chain policy checks, use the following command:

```bash
# Install dependencies
pnpm install --no-frozen-lockfile

# Or use the helper script
./pnpm-install.sh
```

### Development

```bash
# Start dashboard dev server
pnpm --filter @borne-map/dashboard dev

# Start driver web app
pnpm --filter @borne-map/driver-web dev

# Start driver mobile app
pnpm --filter @borne-map/driver-mobile dev
```

### Testing

```bash
# Run all tests
pnpm test

# Run mobile tests
pnpm --filter @borne-map/driver-mobile test

# Run web tests
pnpm --filter @borne-map/driver-web test
```

### Build

```bash
# Build all apps
pnpm build

# Build specific app
pnpm --filter @borne-map/dashboard build
```

## Project Structure

```
BorneMap/
├── apps/
│   ├── dashboard/        # Admin/Partner dashboard (Vite + React)
│   ├── driver-web/       # Driver web app (Vite + React)
│   └── driver-mobile/    # Driver mobile app (Expo + React Native)
├── packages/
│   └── ui/               # Shared design system and components
└── specs/                # Specifications and planning docs
```

## Tech Stack

- **Frontend**: React 19, TypeScript 5.7+
- **Web**: Vite 6, React Router v7, Tailwind CSS 4
- **Mobile**: Expo 54, React Native 0.81
- **Testing**: Jest (mobile), Vitest (web)
- **Monorepo**: pnpm workspaces

## Documentation

- [Specifications](/specs/)
- [Design System](/packages/ui/)
- [Dashboard App](/apps/dashboard/)
- [Driver Web App](/apps/driver-web/)
- [Driver Mobile App](/apps/driver-mobile/)

## Contributing

See [AGENTS.md](./AGENTS.md) for development guidelines.
