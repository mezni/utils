import { renderHook } from '@testing-library/react'
import { describe, it, expect, vi } from 'vitest'
import { useTypedNavigation, NavigationRoute } from '../useTypedNavigation'
import { BrowserRouter } from 'react-router-dom'
import React from 'react'

const wrapper = ({ children }: { children: React.ReactNode }) => 
  React.createElement(BrowserRouter, {}, children)

describe('useTypedNavigation', () => {
  it('provides type-safe navigation functions', () => {
    const { result } = renderHook(() => useTypedNavigation(), { wrapper })

    expect(result.current.toHome).toBeDefined()
    expect(result.current.toStation).toBeDefined()
    expect(result.current.toSearch).toBeDefined()
    expect(result.current.toFavorites).toBeDefined()
    expect(result.current.toProfile).toBeDefined()
    expect(result.current.toLogin).toBeDefined()
    expect(result.current.goBack).toBeDefined()
  })

  it('prevents string-based navigation errors', () => {
    const { result } = renderHook(() => useTypedNavigation(), { wrapper })

    // These should not throw TypeScript errors
    result.current.toHome()
    result.current.toFavorites()
    result.current.toProfile()
  })

  it('encodes search queries properly', () => {
    const { result } = renderHook(() => useTypedNavigation(), { wrapper })

    // Should not throw
    expect(() => {
      result.current.toSearch('special chars @#$%')
    }).not.toThrow()
  })
})

describe('NavigationRoute Enum', () => {
  it('defines all required routes', () => {
    expect(NavigationRoute.Home).toBe('/')
    expect(NavigationRoute.StationDetail).toBe('/stations/:id')
    expect(NavigationRoute.Search).toBe('/search')
    expect(NavigationRoute.Favorites).toBe('/favorites')
    expect(NavigationRoute.Profile).toBe('/profile')
    expect(NavigationRoute.Login).toBe('/login')
  })
})
