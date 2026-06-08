<!-- SPECKIT START -->
Current plan: specs/005-frontend-apps-scaffold/plan.md
Scaffold three frontend apps — Driver Web (Vite+React+Leaflet), Driver Mobile (Expo+react-native-maps), Dashboard (Vite+React+AppShell) — with maps, station markers from real API, location handling, and sidebar navigation.
<!-- SPECKIT END -->

## Design Context

- **Register**: product (driver apps + admin dashboard)
- **North Star**: *The Quiet Dashboard* — calm, reliable, flat-by-default, refined and restrained
- **Palette**: Pine & Moss (earthy greens: Pine `#007943`, Deep Pine `#166534`, Moss Tint `#EAF0E6`)
- **Principle**: Green accent on ≤10% of any screen. No shadows on resting surfaces. System font stack. Max border-radius 8px.
- **Tokens**: `PRODUCT.md` (strategy), `DESIGN.md` (visual spec + frontmatter), `.impeccable/design.json` (sidecar for live panel)
- **Live mode**: Configured at `.impeccable/live/config.json` (Vite SPA, driver-web + dashboard)
