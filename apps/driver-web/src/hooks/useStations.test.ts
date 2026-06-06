import { renderHook } from '@testing-library/react'
import { describe, it, expect } from 'vitest'
import { useStations } from './useStations'

describe('useStations', () => {
  it('returns all stations', () => {
    const { result } = renderHook(() => useStations())
    expect(result.current.stations.length).toBe(15)
  })

  it('gets station by id', () => {
    const { result } = renderHook(() => useStations())
    const station = result.current.getStationById('STN-001')
    expect(station).toBeDefined()
    expect(station?.name).toBe('Station de recharge Ariana')
  })

  it('returns undefined for unknown station id', () => {
    const { result } = renderHook(() => useStations())
    expect(result.current.getStationById('STN-999')).toBeUndefined()
  })

  it('returns chargers for a station', () => {
    const { result } = renderHook(() => useStations())
    const chargers = result.current.getChargersForStation('STN-001')
    expect(chargers.length).toBe(3)
  })

  it('returns empty array for station with no chargers', () => {
    const { result } = renderHook(() => useStations())
    expect(result.current.getChargersForStation('STN-999')).toEqual([])
  })

  it('returns reviews for a station', () => {
    const { result } = renderHook(() => useStations())
    const reviews = result.current.getReviewsForStation('STN-001')
    expect(reviews.length).toBe(5)
  })

  it('returns empty array for station with no reviews', () => {
    const { result } = renderHook(() => useStations())
    expect(result.current.getReviewsForStation('STN-999')).toEqual([])
  })
})
