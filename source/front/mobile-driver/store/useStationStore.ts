import { create } from 'zustand'
import { persist } from 'zustand/middleware'
import { Station } from '../services/stationListService'
import { Station as StationDetail } from '../services/stationDetailService'

interface StationState {
  stations: Station[]
  currentPage: number
  totalPages: number
  totalStations: number
  isLoading: boolean
  error: string | null
  selectedStation: Station | null
  currentStationDetail: StationDetail | null
  filters: {
    searchQuery: string
    chargerType?: 'CCS' | 'CHAdeMO' | 'AC'
    minPower?: number
    maxPower?: number
    available?: boolean
  }

  // Actions
  setStations: (stations: Station[]) => void
  setCurrentPage: (page: number) => void
  setTotalPages: (pages: number) => void
  setTotalStations: (total: number) => void
  setLoading: (loading: boolean) => void
  setError: (error: string | null) => void
  setSelectedStation: (station: Station | null) => void
  setCurrentStationDetail: (station: StationDetail | null) => void
  setSearchQuery: (query: string) => void
  setFilters: (filters: Partial<StationState['filters']>) => void
  resetFilters: () => void
}

const INITIAL_FILTERS = {
  searchQuery: '',
  chargerType: undefined,
  minPower: undefined,
  maxPower: undefined,
  available: undefined,
}

export const useStationStore = create<StationState>()(
  persist(
    (set) => ({
      stations: [],
      currentPage: 1,
      totalPages: 1,
      totalStations: 0,
      isLoading: false,
      error: null,
      selectedStation: null,
      currentStationDetail: null,
      filters: INITIAL_FILTERS,

      setStations: (stations) => set({ stations }),
      setCurrentPage: (page) => set({ currentPage: page }),
      setTotalPages: (pages) => set({ totalPages: pages }),
      setTotalStations: (total) => set({ totalStations: total }),
      setLoading: (loading) => set({ isLoading: loading }),
      setError: (error) => set({ error }),
      setSelectedStation: (station) => set({ selectedStation: station }),
      setCurrentStationDetail: (station) => set({ currentStationDetail: station }),
      setSearchQuery: (query) => set((state) => ({ filters: { ...state.filters, searchQuery: query } })),
      setFilters: (filters) => set((state) => ({
        filters: { ...state.filters, ...filters }
      })),
      resetFilters: () => set({ filters: INITIAL_FILTERS }),
    }),
    {
      name: 'station-storage',
    }
  )
)

// Helper functions for station operations
export function calculateDistance(
  lat1: number,
  lng1: number,
  lat2: number,
  lng2: number,
): number {
  const R = 6371 // Earth's radius in kilometers
  const dLat = (lat2 - lat1) * (Math.PI / 180)
  const dLng = (lng2 - lng1) * (Math.PI / 180)
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * (Math.PI / 180)) *
      Math.cos(lat2 * (Math.PI / 180)) *
      Math.sin(dLng / 2) * Math.sin(dLng / 2)
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  return R * c
}

export function getStationDistance(
  station: Station,
  userLat: number,
  userLng: number,
): number {
  if (!station.geometry || station.geometry.type !== 'Point') {
    return 0
  }

  const [lng, lat] = station.geometry.coordinates
  return calculateDistance(userLat, userLng, lat, lng)
}

export function getFilteredStations(
  stations: Station[],
  filters: StationState['filters'],
  userLocation?: { lat: number; lng: number },
): Station[] {
  let filtered = [...stations]

  // Apply search filter
  if (filters.searchQuery) {
    const searchTerm = filters.searchQuery.toLowerCase()
    filtered = filtered.filter(
      (station) =>
        station.name.toLowerCase().includes(searchTerm) ||
        station.address.toLowerCase().includes(searchTerm),
    )
  }

  // Apply charger type filter
  const chargerType = filters.chargerType
  if (chargerType) {
    filtered = filtered.filter((station) =>
      station.amenities.includes(chargerType),
    )
  }

  // Apply power range filter
  if (filters.minPower !== undefined) {
    filtered = filtered.filter((station) => {
      // Simulated power based on amenities
      const power = station.amenities.includes('CCS') ? 50 : 7
      return power >= filters.minPower!
    })
  }

  // Apply availability filter
  if (filters.available !== undefined) {
    // Simulated availability based on name
    filtered = filtered.filter((station) =>
      station.name.toLowerCase().includes('available'),
    )
  }

  // Add distance if user location is available
  if (userLocation) {
    filtered = filtered.map((station) => ({
      ...station,
      distance_km: getStationDistance(station, userLocation.lat, userLocation.lng).toFixed(2),
    }))
  }

  return filtered
}

export function formatDistance(distance: number): string {
  if (distance < 1) {
    return `${(distance * 1000).toFixed(0)}m`
  }
  return `${distance.toFixed(1)}km`
}
