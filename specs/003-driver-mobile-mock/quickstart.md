# Quickstart: Driver Mobile App

## Prerequisites

- Node.js 20+
- pnpm 9+
- Expo Go (iOS/Android) or simulator/emulator
- Xcode (for iOS simulator) or Android Studio (for Android emulator)

## Setup

```bash
# Install dependencies (from monorepo root)
pnpm install

# Start the mobile app
pnpm --filter @bornemap/driver-mobile dev
```

Or directly:

```bash
cd apps/driver-mobile
pnpm dev
```

## Running

The `pnpm dev` command runs `npx expo start` which starts the Expo dev server. From here you can:

- Press `i` to open in iOS simulator (macOS only)
- Press `a` to open in Android emulator
- Scan the QR code with Expo Go on your device

## Testing

```bash
# Run all tests
pnpm --filter @bornemap/driver-mobile test

# Watch mode
pnpm --filter @bornemap/driver-mobile test -- --watch
```

## Build

```bash
# Web bundle (production)
pnpm --filter @bornemap/driver-mobile build

# EAS Build (iOS/Android binaries)
npx eas build --platform ios
npx eas build --platform android
```

## Environment

No environment configuration needed — all data comes from local mock files. No backend connection required.

## RTL Testing

To test Arabic RTL layout:
1. Change device language to Arabic (Settings → Language → العربية)
2. The app detects locale via `expo-localization` and renders in RTL
3. Verify all screens: Map, Station List, Station Detail, Search, Favorites, Profile, Login/Register

## Troubleshooting

| Problem | Solution |
|---------|----------|
| Metro bundler not starting | `npx expo start --clear` |
| Fonts not loading | `npx expo install expo-font @expo-google-fonts/plus-jakarta-sans` |
| Safe area insets missing | Verify `react-native-safe-area-context` is installed |
| Navigation errors | Run `npx expo install react-native-screens react-native-safe-area-context` |
