import { renderHook, act } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { useMockFilter } from './useMockFilter'

describe('useMockFilter', () => {
  it('returns all stations by default', () => {
    const { result } = renderHook(() => useMockFilter())
    expect(result.current.filteredStations.length).toBe(15)
  })

  it('filters by availability', () => {
    const { result } = renderHook(() => useMockFilter())
    act(() => { result.current.setAvailability('available') })
    const unavailableStations = result.current.filteredStations.filter(s => s.availability === 'unavailable')
    expect(unavailableStations.length).toBe(0)
  })

  it('filters by search query matching name', () => {
    const { result } = renderHook(() => useMockFilter())
    act(() => { result.current.setSearchQuery('Ariana') })
    expect(result.current.filteredStations.length).toBeGreaterThan(0)
    result.current.filteredStations.forEach(s => {
      const isInName = s.name.toLowerCase().includes('ariana')
      const isInAddress = s.address.toLowerCase().includes('ariana')
      expect(isInName || isInAddress).toBe(true)
    })
  })

  it('filters by search query matching address', () => {
    const { result } = renderHook(() => useMockFilter())
    act(() => { result.current.setSearchQuery('Bourguiba') })
    expect(result.current.filteredStations.length).toBeGreaterThan(0)
  })

  it('returns empty when search matches nothing', () => {
    const { result } = renderHook(() => useMockFilter())
    act(() => { result.current.setSearchQuery('ZZZZZZNONEXISTENT') })
    expect(result.current.filteredStations.length).toBe(0)
  })

  it('sets charger type filter', () => {
    const { result } = renderHook(() => useMockFilter())
    act(() => { result.current.setChargerType('CCS') })
    expect(result.current.filter.chargerType).toBe('CCS')
  })

  it('resets search query with empty string', () => {
    const { result } = renderHook(() => useMockFilter())
    act(() => { result.current.setSearchQuery('Tunis') })
    expect(result.current.filteredStations.length).toBeGreaterThan(0)
    act(() => { result.current.setSearchQuery('') })
    expect(result.current.filteredStations.length).toBe(15)
  })
})
