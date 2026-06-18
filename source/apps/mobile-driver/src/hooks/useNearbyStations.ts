import { useQuery } from '@tanstack/react-query'
import { useCallback, useState } from 'react'
import { fetchNearbyStations } from '../services/api'
import { writeCache, readCache } from '../cache/asyncStorage'
import { isOnline } from '../utils/network'
import { Station } from '../types'

interface UseNearbyStationsResult {
  stations: Station[]
  isLoading: boolean
  isError: boolean
  error: Error | null
  refetch: () => void
  isFetching: boolean
  isOffline: boolean
  cachedStations: Station[] | null
}

export function useNearbyStations(
  lat: number,
  lng: number,
  radius: number = 10000,
): UseNearbyStationsResult {
  const [isOffline, setIsOffline] = useState(false)
  const [cachedStations, setCachedStations] = useState<Station[] | null>(null)

  const { data, isLoading, isError, error, refetch, isFetching } = useQuery({
    queryKey: ['nearbyStations', lat, lng, radius],
    queryFn: async ({ signal }) => {
      setIsOffline(false)

      const online = await isOnline()
      if (!online) {
        const cached = await readCache(lat, lng)
        if (cached && cached.length > 0) {
          setIsOffline(true)
          setCachedStations(cached)
          return cached
        }
        throw new Error('No network connection and no cached data available')
      }

      const stations = await fetchNearbyStations(lat, lng, radius, signal)
      writeCache(lat, lng, stations)
      return stations
    },
    enabled: true,
    staleTime: 60000,
    retry: 2,
    retryDelay: (attemptIndex) => Math.min(1000 * 2 ** attemptIndex, 10000),
  })

  const handleRefetch = useCallback(() => {
    setIsOffline(false)
    setCachedStations(null)
    refetch()
  }, [refetch])

  return {
    stations: data ?? [],
    isLoading,
    isError,
    error: error as Error | null,
    refetch: handleRefetch,
    isFetching,
    isOffline,
    cachedStations,
  }
}
