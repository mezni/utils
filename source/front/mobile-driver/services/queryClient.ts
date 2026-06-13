import { QueryClient, useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import ENV from '../config/env'

// Create a single client for the entire app
export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5 * 60 * 1000, // 5 minutes
      refetchOnWindowFocus: false,
      retry: 2,
      gcTime: 10 * 60 * 1000, // 10 minutes
    },
    mutations: {
      retry: 1,
    },
  },
})

const API_BASE_URL = ENV.API_BASE_URL

// Station list queries
export function useStations(page: number = 1, perPage: number = 20, searchQuery: string = '') {
  return useQuery({
    queryKey: ['stations', page, perPage, searchQuery],
    queryFn: async () => {
      const params = new URLSearchParams({
        page: page.toString(),
        per_page: perPage.toString(),
      })
      if (searchQuery) {
        params.append('search', searchQuery)
      }

      const response = await fetch(`${API_BASE_URL}/api/v1/stations?${params}`)
      if (!response.ok) {
        throw new Error('Failed to fetch stations')
      }
      return response.json()
    },
  })
}

// Station detail query
export function useStationDetail(id: string | undefined) {
  return useQuery({
    queryKey: ['station', id],
    queryFn: async () => {
      if (!id) {
        throw new Error('Station ID is required')
      }

      const response = await fetch(`${API_BASE_URL}/api/v1/stations/${id}`)
      if (!response.ok) {
        if (response.status === 404) {
          throw new Error('Station not found')
        }
        throw new Error('Failed to fetch station')
      }
      return response.json()
    },
    enabled: !!id,
  })
}

// Nearby stations query
export function useNearbyStations(lat: number, lng: number, radius: number = 10) {
  return useQuery({
    queryKey: ['nearby', lat, lng, radius],
    queryFn: async () => {
      const params = new URLSearchParams({
        lat: lat.toString(),
        lng: lng.toString(),
        radius: radius.toString(),
      })

      const response = await fetch(`${API_BASE_URL}/api/v1/stations/nearby?${params}`)
      if (!response.ok) {
        throw new Error('Failed to fetch nearby stations')
      }
      return response.json()
    },
  })
}

// Mutation hook for favorites
export function useToggleFavorite() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async (stationId: string) => {
      // TODO: Implement favorite toggle API
      console.log('Toggle favorite:', stationId)
      return { success: true }
    },
    onSuccess: () => {
      // Invalidate relevant queries
      queryClient.invalidateQueries({ queryKey: ['stations'] })
    },
  })
}

// Mutation hook for refresh
export function useRefresh() {
  const queryClient = useQueryClient()

  return {
    refresh: async () => {
      await queryClient.invalidateQueries({ queryKey: ['stations', 'nearby', 'station'] })
    },
  }
}
