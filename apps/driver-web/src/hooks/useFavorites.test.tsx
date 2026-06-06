import { renderHook, act } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { FavoritesProvider, useFavorites } from './useFavorites'
import type { ReactNode } from 'react'

function wrapper({ children }: { children: ReactNode }) {
  return <FavoritesProvider initialFavorites={['STN-001', 'STN-003']}>{children}</FavoritesProvider>
}

describe('useFavorites', () => {
  it('returns initial favorites', () => {
    const { result } = renderHook(() => useFavorites(), { wrapper })
    expect(result.current.favorites).toEqual(['STN-001', 'STN-003'])
  })

  it('checks if id is favorite', () => {
    const { result } = renderHook(() => useFavorites(), { wrapper })
    expect(result.current.isFavorite('STN-001')).toBe(true)
    expect(result.current.isFavorite('STN-999')).toBe(false)
  })

  it('toggles favorite on', () => {
    const { result } = renderHook(() => useFavorites(), { wrapper })
    act(() => { result.current.toggleFavorite('STN-005') })
    expect(result.current.favorites).toContain('STN-005')
  })

  it('toggles favorite off', () => {
    const { result } = renderHook(() => useFavorites(), { wrapper })
    act(() => { result.current.toggleFavorite('STN-001') })
    expect(result.current.favorites).not.toContain('STN-001')
  })

  it('toggles same id twice returns to original', () => {
    const { result } = renderHook(() => useFavorites(), { wrapper })
    act(() => { result.current.toggleFavorite('STN-001') })
    act(() => { result.current.toggleFavorite('STN-001') })
    expect(result.current.favorites).toContain('STN-001')
  })
})
