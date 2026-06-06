import { useMemo, useState, useCallback } from 'react'
import { stations } from '../mocks/stations'
import { chargers } from '../mocks/chargers'
import { reviews } from '../mocks/reviews'
import type { Charger, Review, Station } from '../types'

export interface UseStationsReturn {
  stations: Station[]
  loading: boolean
  error: Error | null
  getStationById: (id: string) => Station | undefined
  getChargersForStation: (stationId: string) => Charger[]
  getReviewsForStation: (stationId: string) => Review[]
  retry: () => Promise<void>
}

/**
 * Hook for managing station data
 * Currently uses mock data, but structured to support real API
 * @returns Station management interface
 */
export function useStations(): UseStationsReturn {
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<Error | null>(null)

  const data = useMemo(
    () => ({
      stations,
      getStationById(id: string) {
        return stations.find(s => s.id === id)
      },
      getChargersForStation(stationId: string): Charger[] {
        return chargers.filter(c => c.stationId === stationId)
      },
      getReviewsForStation(stationId: string): Review[] {
        return reviews.filter(r => r.stationId === stationId)
      },
    }),
    [],
  )

  const retry = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      // In a real implementation, fetch from API here
      // const data = await fetchStations()
      // setStations(data)
      setLoading(false)
    } catch (err) {
      setError(err instanceof Error ? err : new Error('Failed to fetch stations'))
      setLoading(false)
    }
  }, [])

  return {
    stations: data.stations,
    loading,
    error,
    getStationById: data.getStationById,
    getChargersForStation: data.getChargersForStation,
    getReviewsForStation: data.getReviewsForStation,
    retry,
  }
}
