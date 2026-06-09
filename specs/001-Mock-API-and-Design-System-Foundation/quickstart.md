# Quickstart: Mock API and Design System Foundation

**Date**: 2026-06-09

## Prerequisites

- Node.js 18+ (verify: `node --version`)
- pnpm (verify: `pnpm --version`)

## Setup

```bash
# Install dependencies
pnpm install

# Start the mock API server
pnpm mock
```

The mock server starts on `http://localhost:3001`. All endpoints are under the `/api` prefix.

## Verify Mock API

```bash
# List all partners
curl http://localhost:3001/api/partners

# Filter stations by partner
curl "http://localhost:3001/api/stations?partner_id=PRT001"

# Filter chargers by station
curl "http://localhost:3001/api/chargers?station_id=STN001"

# List availability records
curl http://localhost:3001/api/station_availability

# Health check
curl http://localhost:3001/api/partners/1
```

## Verify Design Tokens

```bash
# TypeScript compilation check for token files
npx tsc --noEmit source/packages/ui/src/tokens/colors.ts
npx tsc --noEmit source/packages/ui/src/tokens/native.ts

# Verify native.ts matches colors.ts (no diff)
diff <(grep -oP "'.*?'" source/packages/ui/src/tokens/colors.ts) \
     <(grep -oP "'.*?'" source/packages/ui/src/tokens/native.ts)
```

## Dev Commands

| Command | Description |
|---------|-------------|
| `pnpm mock` | Start json-server on port 3001 |
| `pnpm dev:dashboard` | Start Dashboard dev server |
| `pnpm dev:web` | Start Driver Web dev server |
| `pnpm dev:mobile` | Start Driver Mobile dev server |
| `pnpm dev` | List all available dev commands |

## Project Structure Created in This Sprint

```
source/
├── mock/
│   ├── db.json           # 4 resources, seeded data
│   └── routes.json       # /api/* → /$1 rewrite
└── packages/
    └── ui/
        ├── package.json
        ├── tsconfig.json
        ├── tailwind.config.base.js
        └── src/
            └── tokens/
                ├── colors.ts
                ├── typography.ts
                ├── spacing.ts
                ├── radius.ts
                ├── shadows.ts
                ├── native.ts
                └── index.ts
```

## Expected Results

- `GET /api/partners` → Array of 3 partners with all fields
- `GET /api/stations?partner_id=PRT001` → Only partner 1 stations
- `GET /api/chargers?station_id=STN001` → Only station 1 chargers
- `import { colors } from '@borne-map/ui'` → brand.primary = '#007943'
