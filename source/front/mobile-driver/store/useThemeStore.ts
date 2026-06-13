import { create } from 'zustand'
import AsyncStorage from '@react-native-async-storage/async-storage'

interface ThemeState {
  isDarkMode: boolean
  toggleTheme: () => void
  loadTheme: () => Promise<void>
}

const THEME_KEY = 'theme_preference'

export const useThemeStore = create<ThemeState>((set) => ({
  isDarkMode: false,
  toggleTheme: () =>
    set((state) => {
      const newMode = !state.isDarkMode
      return { isDarkMode: newMode }
    }),
  loadTheme: async () => {
    try {
      const savedTheme = await AsyncStorage.getItem(THEME_KEY)
      if (savedTheme) {
        set({ isDarkMode: savedTheme === 'dark' })
      }
    } catch (error) {
      console.error('Failed to load theme:', error)
      // Default to light mode on error
      set({ isDarkMode: false })
    }
  },
}))

// Initialize theme on app load
export const initializeTheme = async () => {
  await useThemeStore.getState().loadTheme()
}

// Subscribe to theme changes to persist
export const subscribeToThemeChanges = () => {
  const unsubscribe = useThemeStore.subscribe((state) => {
    AsyncStorage.setItem(THEME_KEY, state.isDarkMode ? 'dark' : 'light')
  })

  return unsubscribe
}
