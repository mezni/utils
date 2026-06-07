# Onboarding Guide

## Setup

```bash
git clone https://github.com/mezni/BorneMap.git
cd BorneMap
pnpm install --no-frozen-lockfile
```

## Running Apps

### Driver Web
```bash
cd apps/driver-web
pnpm dev
```
URL: http://localhost:5173

### Driver Mobile
```bash
cd apps/driver-mobile
pnpm dev
```
Or for iOS/Android:
```bash
npx expo start --ios
npx expo start --android
```

### Dashboard
```bash
cd apps/dashboard
pnpm dev
```
URL: http://localhost:5174

## Language Switching

1. Open the Profile/Settings screen
2. Select language: العربية (Arabic), Français (French), or English
3. RTL layout applies automatically for Arabic

## Role Switching (Dashboard)

1. Click the user icon in the TopBar
2. Select Partner or Admin role
3. Sidebar navigation updates automatically

## Building Apps

```bash
cd apps/driver-web && pnpm build
cd apps/driver-mobile && pnpm build
cd apps/dashboard && pnpm build
```

## Testing

### RTL Testing
- Set language to Arabic
- Verify all 25 screens in all 3 apps
- Check sidebar, tables, forms, buttons, icons

### Accessibility Testing
- Run Lighthouse audit in Chrome DevTools
- Use axe DevTools extension
- Verify keyboard navigation (Tab, Enter, Escape)
- Check focus indicators are visible

### Cross-Browser Testing
- Test on Chrome, Firefox, and Safari
- Verify no console errors, layout shifts, or feature gaps

### Mobile Testing
- Use iOS Simulator (Xcode) and Android Simulator (Android Studio)
- Verify all 7 screens, touch targets ≥ 44x44, safe area insets

## Troubleshooting

### App doesn't start
```bash
rm -rf node_modules apps/*/node_modules
pnpm install --no-frozen-lockfile
```

### RTL layout broken
- Verify `documentElement.dir = 'rtl'` is set
- Check for hardcoded margin-left/right instead of margin-inline-start/end

### Build fails (TypeScript)
- Run `pnpm build` on individual packages first: `packages/ui`, then app
- Check for missing token imports in components
