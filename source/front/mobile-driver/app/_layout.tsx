import { StatusBar } from 'expo-status-bar'
import { Stack } from 'expo-router'
import { QueryClientProvider } from '@tanstack/react-query'
import { useThemeStore } from '../store/useThemeStore'
import { queryClient } from '../services/queryClient'

export default function RootLayout() {
  const { isDarkMode } = useThemeStore()

  return (
    <QueryClientProvider client={queryClient}>
      <Stack screenOptions={{ headerShown: false }}>
        <Stack.Screen name="index" options={{ headerShown: false }} />
        <Stack.Screen name="stations" options={{ headerShown: false }} />
        <Stack.Screen name="station/[id]" options={{ headerShown: false }} />
      </Stack>
    </QueryClientProvider>
  )
}
