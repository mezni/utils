import { create } from 'zustand'

export interface User {
  id: string
  email: string
  role: 'admin' | 'user'
  created_at: string
}

interface AuthState {
  accessToken: string | null
  refreshToken: string | null
  user: User | null
  isAuthenticated: boolean
  setTokens: (access: string, refresh: string) => void
  setUser: (user: User) => void
  logout: () => void
  hydrate: () => void
}

export const useAuthStore = create<AuthState>((set) => ({
  accessToken: null,
  refreshToken: null,
  user: null,
  isAuthenticated: false,

  setTokens: (access, refresh) => {
    localStorage.setItem('access_token', access)
    localStorage.setItem('refresh_token', refresh)
    set({ accessToken: access, refreshToken: refresh, isAuthenticated: true })
  },

  setUser: (user) => {
    localStorage.setItem('user', JSON.stringify(user))
    set({ user })
  },

  logout: () => {
    localStorage.removeItem('access_token')
    localStorage.removeItem('refresh_token')
    localStorage.removeItem('user')
    set({ accessToken: null, refreshToken: null, user: null, isAuthenticated: false })
  },

  hydrate: () => {
    const accessToken = localStorage.getItem('access_token')
    const refreshToken = localStorage.getItem('refresh_token')
    const userRaw = localStorage.getItem('user')
    const user = userRaw ? JSON.parse(userRaw) : null
    set({ accessToken, refreshToken, user, isAuthenticated: !!accessToken })
  },
}))
