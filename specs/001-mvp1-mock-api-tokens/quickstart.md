# Quickstart: MVP-1 Foundation Setup

**Prerequisites**: Node.js >= 18, pnpm >= 9

## Setup

```bash
# Install dependencies (from repository root)
pnpm install

# Start the mock API
pnpm mock

# In another terminal, verify the API
curl http://localhost:3001/api/stations | jq '. | length'
# → 15

curl http://localhost:3001/api/partners | jq '. | length'
# → 3

curl http://localhost:3001/api/chargers | jq '. | length'
# → 24

# Filter stations by partner
curl 'http://localhost:3001/api/stations?partner_id=1'

# Filter chargers by station
curl 'http://localhost:3001/api/chargers?station_id=1'
```

## Running Everything

```bash
# Start mock API + dashboard app concurrently
pnpm dev
```

## Verification Checklist

- [ ] `pnpm mock` starts json-server on port 3001
- [ ] `GET /api/stations` returns 15 stations with coordinates
- [ ] `GET /api/partners` returns 3 partners
- [ ] `GET /api/chargers` returns 24 chargers
- [ ] `GET /api/stations?partner_id=1` returns filtered results
- [ ] `GET /api/chargers?station_id=1` returns filtered results
- [ ] All token files compile: `npx tsc --noEmit source/packages/ui/src/tokens/*.ts`
- [ ] Brand primary `#007943` is accessible from Tailwind config

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Port 3001 in use | Edit root `package.json` — change `--port 3001` to available port |
| `pnpm mock` not found | Run `pnpm install` first |
| `jq` not installed | Omit `| jq` from curl commands — raw JSON output still works |
