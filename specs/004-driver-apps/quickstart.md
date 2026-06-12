# Quickstart: Mobile & Web Driver Apps

**Feature**: MVP-1 Phase 4 - Mobile & Web Driver Apps
**Branch**: `004-driver-apps`
**Date**: 2026-06-12

## Prerequisites

Before starting, ensure you have:

- **Node.js**: v18+ (for both mobile and web apps)
- **pnpm**: Latest version (workspace manager)
- **Expo CLI**: `npm install -g expo-cli` (mobile app only)
- **Docker**: Running services (driver-service on :8080)
- **Git**: For version control
- **Code Editor**: VS Code recommended (with extensions: TypeScript, ESLint, Prettier)

---

## Development Setup

### 1. Clone Repository

```bash
git clone https://github.com/mezni/BorneMap.git
cd BorneMap
```

### 2. Install Dependencies

```bash
# Install all workspace dependencies
pnpm install
```

### 3. Verify Backend Services

Ensure driver-service is running:

```bash
# Check driver-service health
curl http://localhost:8080/api/v1/health

# Expected response:
# {"status":"healthy","timestamp":"2026-06-12T14:30:00Z","version":"1.0.0"}
```

If not running, start from root:
```bash
docker-compose up -d driver-service
```

### 4. Verify Design System

Ensure design system packages are built:

```bash
cd source/front

# Build tokens package
pnpm build:tokens

# Build UI package
pnpm build:ui

# Verify both packages exist
ls -la packages/tokens/dist/
ls -la packages/ui/dist/
```

---

## Mobile Driver App (Expo)

### 1. Initialize Project

```bash
cd source/front/mobile-driver
```

### 2. Install Dependencies

```bash
pnpm install
```

**Dependencies Installed**:
- expo@~54.0.0
- expo-router@3.0.0
- zustand@4.0.0
- @tanstack/react-query@5.0.0
- react-native-reanimated@3.0.0
- @react-native-async-storage/async-storage@2.0.0
- react-native-maps@11.0.0
- @bornemap/ui
- @bornemap/tokens
- NativeWind (Tailwind for React Native)

### 3. Configure Project

**App Configuration** (`app.json`):

```json
{
  "expo": {
    "name": "BorneMap Driver",
    "slug": "bornemap-driver",
    "version": "1.0.0",
    "orientation": "portrait",
    "icon": "./assets/icon.png",
    "userInterfaceStyle": "automatic",
    "splash": {
      "image": "./assets/splash.png",
      "resizeMode": "contain",
      "backgroundColor": "#ffffff"
    },
    "assetBundlePatterns": ["**/*"],
    "ios": {
      "supportsTablet": false,
      "bundleIdentifier": "com.bornemap.driver",
      "buildNumber": "1"
    },
    "android": {
      "adaptiveIcon": {
        "foregroundImage": "./assets/adaptive-icon.png",
        "backgroundColor": "#ffffff"
      },
      "package": "com.bornemap.driver",
      "versionCode": 1
    },
    "web": {
      "favicon": "./assets/favicon.png"
    },
    "plugins": ["expo-router"]
  }
}
```

### 4. Run Development Server

**Option 1: iOS Simulator** (macOS only):

```bash
pnpm ios
```

**Option 2: Android Emulator** (requires Android Studio):

```bash
pnpm android
```

**Option 3: Web (Expo Go)**:

```bash
pnpm web
```

### 5. Build for Production

**iOS**:

```bash
eas build --platform ios
```

**Android**:

```bash
eas build --platform android
```

**Web**:

```bash
eas build --platform web
```

### 6. Test Mobile App

**Manual Testing Checklist**:
- [ ] App launches successfully
- [ ] Map renders without errors
- [ ] Geolocation permission requested (iOS/Android)
- [ ] Markers appear for nearby stations
- [ ] Tap marker → Station detail screen opens
- [ ] Pull-to-refresh works on map
- [ ] Dark mode toggle works
- [ ] Haptic feedback on primary actions
- [ ] Navigation button opens external app

**Device Testing**:
- iPhone 13 (primary)
- Samsung Galaxy (primary)
- Test on both light and dark mode

---

## Web Driver App (React + Vite)

### 1. Initialize Project

```bash
cd source/front/web-driver
```

### 2. Install Dependencies

```bash
pnpm install
```

**Dependencies Installed**:
- react@18
- react-dom@18
- vite@5.0.0
- react-router-dom@6
- zustand@4.0.0
- @tanstack/react-query@5.0.0
- leaflet@1.9.0
- @bornemap/ui
- @bornemap/tokens

### 3. Configure Project

**Vite Configuration** (`vite.config.ts`):

```typescript
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://localhost:8080',
        changeOrigin: true,
      },
    },
  },
})
```

**Environment Variables** (`.env`):

```env
VITE_API_BASE_URL=http://localhost:8080/api/v1
VITE_OSM_NOMINATIM_URL=https://nominatim.openstreetmap.org/search
```

### 4. Run Development Server

```bash
pnpm dev
```

**Server starts at**: http://localhost:5173

### 5. Build for Production

```bash
pnpm build
```

**Output**: `dist/` directory

**Deploy to**:
- Netlify (drag and drop `dist/`)
- Vercel (`vercel --prod`)
- Any static hosting provider

### 6. Test Web App

**Manual Testing Checklist**:
- [ ] App loads successfully in browser
- [ ] Map renders without errors
- [ ] Station list displays correctly
- [ ] Search bar works (address/name)
- [ ] Station detail page opens
- [ ] Dark mode toggle works
- [ ] Pull-to-refresh works
- [ ] Responsive design works (mobile, tablet, desktop)
- [ ] Navigation button opens external app

**Responsive Testing**:
- iPhone SE (375x667)
- iPhone 13 Pro (390x844)
- iPad Pro (1024x1366)
- Desktop (1920x1080)

---

## Common Development Tasks

### Running Tests

```bash
# Mobile app tests
pnpm test

# Web app tests
cd source/front/web-driver
pnpm test
```

### Linting

```bash
# Lint all packages
pnpm lint

# Lint mobile app only
cd source/front/mobile-driver
pnpm lint

# Lint web app only
cd source/front/web-driver
pnpm lint
```

### Type Checking

```bash
# Typecheck all packages
pnpm typecheck

# Typecheck mobile app only
cd source/front/mobile-driver
pnpm typecheck

# Typecheck web app only
cd source/front/web-driver
pnpm typecheck
```

### Bundle Analysis

```bash
# Analyze mobile app bundle
cd source/front/mobile-driver
pnpm analyze-bundle

# Analyze web app bundle
cd source/front/web-driver
pnpm analyze-bundle
```

---

## Development Workflows

### Feature Development

1. **Create Feature Branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Implement Feature**:
   - Write code following project conventions
   - Add tests for new functionality
   - Update documentation

3. **Test Locally**:
   ```bash
   # Mobile app
   pnpm ios  # or pnpm android

   # Web app
   pnpm dev
   ```

4. **Run Lint & Typecheck**:
   ```bash
   pnpm lint && pnpm typecheck
   ```

5. **Commit Changes**:
   ```bash
   git add .
   git commit -m "feat: add your feature description"
   ```

6. **Create Pull Request**:
   - Push to feature branch
   - Create PR on GitHub
   - Request code review

### Bug Fixes

1. **Create Bugfix Branch**:
   ```bash
   git checkout -b fix/your-bug-description
   ```

2. **Implement Fix**:
   - Fix the bug
   - Add regression tests
   - Verify fix works

3. **Test & Commit**:
   ```bash
   pnpm test && pnpm lint
   git add .
   git commit -m "fix: resolve your bug description"
   ```

4. **Create PR** as above.

---

## Troubleshooting

### Mobile App Won't Start

**Issue**: Expo app fails to launch on iOS/Android

**Solutions**:
1. Clear Expo cache:
   ```bash
   expo start -c
   ```

2. Reinstall dependencies:
   ```bash
   rm -rf node_modules
   pnpm install
   ```

3. Check node version:
   ```bash
   node --version  # Must be v18+
   ```

### Web App Build Fails

**Issue**: Vite build fails with errors

**Solutions**:
1. Clear node_modules and cache:
   ```bash
   rm -rf node_modules dist
   pnpm install
   pnpm build
   ```

2. Check TypeScript version compatibility:
   ```bash
   pnpm typecheck
   ```

3. Verify all dependencies are compatible with each other

### API Requests Fail

**Issue**: Frontend can't reach backend API

**Solutions**:
1. Verify driver-service is running:
   ```bash
   curl http://localhost:8080/api/v1/health
   ```

2. Check proxy configuration (web app):
   ```typescript
   // vite.config.ts
   proxy: {
     '/api': {
       target: 'http://localhost:8080',
       changeOrigin: true,
     },
   }
   ```

3. Check CORS configuration (backend):
   ```rust
   // driver-service/src/main.rs
   // Ensure CORS is enabled for frontend origins
   ```

### Type Errors

**Issue**: TypeScript reports type errors

**Solutions**:
1. Update TypeScript:
   ```bash
   pnpm update @types/node
   ```

2. Check strict mode compliance:
   ```bash
   pnpm typecheck
   ```

3. Review type definitions in @bornemap/ui and @bornemap/tokens

---

## Project Structure

```
source/front/
├── packages/
│   ├── tokens/           ← @bornemap/tokens (design tokens)
│   └── ui/               ← @bornemap/ui (UI components)
│
├── mobile-driver/        ← Expo SDK 54 app
│   ├── app/              ← expo-router pages
│   │   ├── _layout.tsx   ← Root layout with ThemeProvider
│   │   ├── index.tsx     ← Map screen
│   │   ├── stations.tsx  ← Station list
│   │   └── station/[id].tsx ← Station detail
│   ├── components/       ← Reusable components
│   ├── hooks/            ← Custom React hooks
│   ├── services/         ← API layer
│   ├── store/            ← Zustand stores
│   ├── theme/            ← Dark mode config
│   ├── navi.ts           ← Navigation service
│   └── package.json
│
└── web-driver/           ← React 19 web app
    ├── src/
    │   ├── pages/        ← Route pages
    │   ├── components/   ← Reusable components
    │   ├── hooks/        ← Custom hooks
    │   ├── services/     ← API layer
    │   ├── store/        ← Zustand stores
    │   ├── App.tsx       ← Root component
    │   └── main.tsx      ← Entry point
    ├── public/           ← Static assets
    ├── package.json
    └── vite.config.ts
```

---

## Environment Variables

### Mobile App

Create `.env` in `source/front/mobile-driver/`:

```env
# API Configuration
API_BASE_URL=http://localhost:8080/api/v1
OSM_NOMINATIM_URL=https://nominatim.openstreetmap.org/search

# App Configuration
APP_NAME=BorneMap Driver
APP_VERSION=1.0.0

# Debug Mode
DEBUG=false
```

### Web App

Create `.env` in `source/front/web-driver/`:

```env
# API Configuration
VITE_API_BASE_URL=http://localhost:8080/api/v1
VITE_OSM_NOMINATIM_URL=https://nominatim.openstreetmap.org/search

# App Configuration
VITE_APP_NAME=BorneMap Driver
VITE_APP_VERSION=1.0.0

# Debug Mode
VITE_DEBUG=false
```

**Note**: Never commit `.env` files to git. They are already in `.gitignore`.

---

## Next Steps

After setup is complete:

1. **Read the Plan**: Review `specs/004-driver-apps/plan.md` for detailed implementation phases
2. **Review Data Model**: Check `specs/004-driver-apps/data-model.md` for entity definitions
3. **Check API Contracts**: See `specs/004-driver-apps/contracts/api.md` for API endpoints
4. **Run Tasks**: Execute `/speckit.tasks` to generate detailed task breakdown
5. **Start Implementation**: Begin with Phase 1 (Project Setup) in the plan

---

## Support & Resources

- **Design System**: `design-system/bornemap/MASTER.md`
- **Project README**: `/README.md`
- **AGENTS.md**: Project-specific development guidance
- **Constitution**: `.specify/memory/constitution.md`

---

## Performance Targets

**Mobile App**:
- First screen load: <3s
- Station list fetch: <200ms
- Station detail load: <200ms
- Bundle size: <5MB
- Performance: 60fps with 1000+ markers

**Web App**:
- First screen load: <2s
- Station list fetch: <200ms
- Bundle size (gzip): <200KB
- Performance: 60fps on all devices

---

## Testing Checklist

**Before committing code**:
- [ ] Lint passes (`pnpm lint`)
- [ ] Typecheck passes (`pnpm typecheck`)
- [ ] Tests pass (`pnpm test`)
- [ ] Manual testing completed on iOS
- [ ] Manual testing completed on Android
- [ ] Manual testing completed on web
- [ ] Performance targets met
- [ ] No console errors
- [ ] Dark mode works on all screens
- [ ] All UX requirements met (skeletons, haptics, etc.)

---

## License

This code is part of the BorneMap project.

**All contributions must comply with the project's constitution and coding standards.**
