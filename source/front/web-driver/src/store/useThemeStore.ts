import { create } from 'zustand'
import { subscribeWithSelector } from 'zustand/middleware'

interface ThemeState {
  isDarkMode: boolean
  toggleTheme: () => void
  loadTheme: () => Promise<void>
}

const THEME_KEY = 'theme_preference'

export const useThemeStore = create<ThemeState>()(
  subscribeWithSelector((set) => ({
    isDarkMode: false,
    toggleTheme: () =>
      set((state) => {
        const newMode = !state.isDarkMode
        return { isDarkMode: newMode }
      }),
    loadTheme: async () => {
      try {
        if (typeof window !== 'undefined') {
          const savedTheme = localStorage.getItem(THEME_KEY)
          if (savedTheme) {
            set({ isDarkMode: savedTheme === 'dark' })
          }
        }
      } catch (error) {
        console.error('Failed to load theme:', error)
        set({ isDarkMode: false })
      }
    },
  })),
)

// Initialize theme on app load
export const initializeTheme = async () => {
  await useThemeStore.getState().loadTheme()
}

// Subscribe to theme changes to persist
export const subscribeToThemeChanges = () => {
  const unsubscribe = useThemeStore.subscribe((state) => {
    if (typeof window !== 'undefined') {
      localStorage.setItem(THEME_KEY, state.isDarkMode ? 'dark' : 'light')
    }
  })

  return unsubscribe
}
