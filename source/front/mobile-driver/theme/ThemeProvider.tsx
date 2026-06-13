import { ReactNode } from 'react'
import { ThemeProvider as BorneMapThemeProvider } from '@bornemap/ui'

interface ThemeProviderProps {
  children: ReactNode
  darkMode?: boolean
  toggleTheme?: () => void
}

export function ThemeProvider({ children, darkMode = false }: ThemeProviderProps) {
  return (
    <BorneMapThemeProvider mode={darkMode ? 'dark' : 'light'}>
      {children}
    </BorneMapThemeProvider>
  )
}
