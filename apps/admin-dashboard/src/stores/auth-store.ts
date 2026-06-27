import { create } from 'zustand'

export interface User {
  id: string
  email: string
  role: 'ADMIN' | 'PARTNER' | 'DRIVER'
  created_at: string
}

interface AuthState {
  accessToken: string | null
  refreshToken: string | null
  user: User | null
  isAuthenticated: boolean
  isLoading: boolean
  setTokens: (access: string, refresh: string) => void
  setUser: (user: User) => void
  logout: () => void
  hydrate: () => void
  clearSession: () => void
  hasRole: (role: string) => boolean
  hasAnyRole: (roles: string[]) => boolean
  hasAllRoles: (roles: string[]) => boolean
}

export const useAuthStore = create<AuthState>((set, get) => ({
  accessToken: null,
  refreshToken: null,
  user: null,
  isAuthenticated: false,
  isLoading: true,

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

  clearSession: () => {
    localStorage.removeItem('access_token')
    localStorage.removeItem('refresh_token')
    localStorage.removeItem('user')
    set({ accessToken: null, refreshToken: null, user: null, isAuthenticated: false })
  },

  hydrate: () => {
    try {
      const accessToken = localStorage.getItem('access_token')
      const refreshToken = localStorage.getItem('refresh_token')
      const userRaw = localStorage.getItem('user')
      const user = userRaw ? JSON.parse(userRaw) : null
      
      // Check if token is expired
      if (accessToken) {
        try {
          const tokenData = JSON.parse(atob(accessToken.split('.')[1]))
          const now = Math.floor(Date.now() / 1000)
          
          if (tokenData.exp < now) {
            // Token is expired, clear session
            get().clearSession()
            set({ isLoading: false })
            return
          }
        } catch (e) {
          // Invalid token format, clear session
          get().clearSession()
          set({ isLoading: false })
          return
        }
      }
      
      set({ 
        accessToken, 
        refreshToken, 
        user, 
        isAuthenticated: !!accessToken,
        isLoading: false 
      })
    } catch (error) {
      console.error('Error hydrating auth state:', error)
      get().clearSession()
      set({ isLoading: false })
    }
  },

  hasRole: (role) => {
    const { user } = get()
    return user?.role === role
  },

  hasAnyRole: (roles) => {
    const { user } = get()
    return user ? roles.includes(user.role) : false
  },

  hasAllRoles: (roles) => {
    const { user } = get()
    return user ? roles.every(role => role === user.role) : false
  },
}))
