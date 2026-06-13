import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { ThemeProvider } from '@bornemap/ui'
import { useThemeStore, initializeTheme, subscribeToThemeChanges } from './store/useThemeStore'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { useEffect } from 'react'

function AppContent() {
  const { isDarkMode, loadTheme, toggleTheme } = useThemeStore()

  useEffect(() => {
    // Initialize theme on mount
    initializeTheme()
    // Subscribe to theme changes
    const unsubscribe = subscribeToThemeChanges()

    return () => {
      unsubscribe()
    }
  }, [])

  return (
    <ThemeProvider theme={isDarkMode ? 'dark' : 'light'}>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<div className="p-8">Map Screen</div>} />
          <Route path="/stations" element={<div className="p-8">Station List</div>} />
          <Route path="/stations/:id" element={<div className="p-8">Station Detail</div>} />
        </Routes>
      </BrowserRouter>
    </ThemeProvider>
  )
}

export default function App() {
  return (
    <QueryClientProvider client={new QueryClient()}>
      <AppContent />
    </QueryClientProvider>
  )
}
