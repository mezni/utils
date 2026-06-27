import { describe, it, expect, beforeEach } from 'vitest'
import { useAuthStore } from '@/stores/auth-store'

describe('auth-store', () => {
  beforeEach(() => {
    localStorage.clear()
    useAuthStore.setState({
      accessToken: null,
      refreshToken: null,
      user: null,
      isAuthenticated: false,
    })
  })

  it('starts unauthenticated', () => {
    const state = useAuthStore.getState()
    expect(state.isAuthenticated).toBe(false)
    expect(state.accessToken).toBeNull()
    expect(state.user).toBeNull()
  })

  it('setTokens updates state and localStorage', () => {
    useAuthStore.getState().setTokens('access-123', 'refresh-456')

    const state = useAuthStore.getState()
    expect(state.accessToken).toBe('access-123')
    expect(state.refreshToken).toBe('refresh-456')
    expect(state.isAuthenticated).toBe(true)
    expect(localStorage.getItem('access_token')).toBe('access-123')
    expect(localStorage.getItem('refresh_token')).toBe('refresh-456')
  })

  it('setUser updates user state and localStorage', () => {
    const user = { id: '1', email: 'test@test.com', role: 'admin' as const, created_at: '2024-01-01' }
    useAuthStore.getState().setUser(user)

    expect(useAuthStore.getState().user).toEqual(user)
    expect(localStorage.getItem('user')).toBe(JSON.stringify(user))
  })

  it('logout clears all state and localStorage', () => {
    useAuthStore.getState().setTokens('a', 'b')
    useAuthStore.getState().setUser({ id: '1', email: 't@t.com', role: 'admin', created_at: '2024-01-01' })
    useAuthStore.getState().logout()

    const state = useAuthStore.getState()
    expect(state.isAuthenticated).toBe(false)
    expect(state.accessToken).toBeNull()
    expect(state.user).toBeNull()
    expect(localStorage.getItem('access_token')).toBeNull()
    expect(localStorage.getItem('refresh_token')).toBeNull()
    expect(localStorage.getItem('user')).toBeNull()
  })

  it('hydrate restores state from localStorage', () => {
    localStorage.setItem('access_token', 'hydrated-access')
    localStorage.setItem('refresh_token', 'hydrated-refresh')
    localStorage.setItem('user', JSON.stringify({ id: '2', email: 'hydrate@test.com', role: 'user', created_at: '2024-06-01' }))

    useAuthStore.getState().hydrate()

    const state = useAuthStore.getState()
    expect(state.accessToken).toBe('hydrated-access')
    expect(state.refreshToken).toBe('hydrated-refresh')
    expect(state.isAuthenticated).toBe(true)
    expect(state.user?.email).toBe('hydrate@test.com')
  })
})
