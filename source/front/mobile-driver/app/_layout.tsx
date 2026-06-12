import { StatusBar } from 'expo-status-bar'
import { Stack } from 'expo-router'
import { useThemeStore } from '../store/useThemeStore'

export default function RootLayout() {
  const { isDarkMode } = useThemeStore()

  return (
    <Stack screenOptions={{ headerShown: false }}>
      <Stack.Screen name="index" options={{ headerShown: false }} />
      <Stack.Screen name="stations" options={{ headerShown: false }} />
      <Stack.Screen name="station/[id]" options={{ headerShown: false }} />
    </Stack>
  )
}
