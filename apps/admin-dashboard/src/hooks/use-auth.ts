import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from '@/lib/api'
import { useAuthStore, type User } from '@/stores/auth-store'
import type { LoginFormData } from '@/lib/validation'

interface LoginResponse {
  access_token: string
  refresh_token: string
  user: User
}

interface AuthError {
  message: string
}

export function useLogin() {
  const setTokens = useAuthStore((s) => s.setTokens)
  const setUser = useAuthStore((s) => s.setUser)
  const queryClient = useQueryClient()

  return useMutation<LoginResponse, AuthError, LoginFormData>({
    mutationFn: async (data) => {
      const res = await api.post<LoginResponse>('/auth/login', data)
      return res.data
    },
    onSuccess(data) {
      setTokens(data.access_token, data.refresh_token)
      setUser(data.user)
      localStorage.setItem('user', JSON.stringify(data.user))
      queryClient.invalidateQueries({ queryKey: ['me'] })
    },
  })
}

export function useLogout() {
  const logout = useAuthStore((s) => s.logout)
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async () => {
      try {
        await api.post('/auth/logout')
      } catch {
        // ignore server errors during logout
      }
    },
    onSettled: () => {
      logout()
      queryClient.clear()
    },
  })
}

export function useCurrentUser() {
  const isAuthenticated = useAuthStore((s) => s.isAuthenticated)

  return useQuery<User>({
    queryKey: ['me'],
    queryFn: async () => {
      const res = await api.get<User>('/auth/me')
      return res.data
    },
    enabled: isAuthenticated,
  })
}
