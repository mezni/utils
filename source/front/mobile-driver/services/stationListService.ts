export interface Station {
  id: string
  name: string
  address: string
  geometry: {
    type: 'Point'
    coordinates: [number, number]
  }
  amenities: string[]
  operating_hours: string
  created_at: string
  updated_at: string
}

export interface StationsResponse {
  data: Station[]
  meta: {
    page: number
    per_page: number
    total: number
    total_pages: number
  }
}

export interface StationListParams {
  page?: number
  per_page?: number
  search?: string
}

export async function fetchStationsWithParams({
  page = 1,
  per_page = 20,
  search = '',
}: StationListParams): Promise<StationsResponse> {
  // This is the actual implementation that will be replaced with real API calls
  // For now, return mock data

  // Simulate network delay
  await new Promise<void>(resolve => setTimeout(() => resolve(), 500))

  const totalStations = 150
  const totalPages = Math.ceil(totalStations / per_page)

  // Generate mock stations based on search
  const filteredStations = Array.from({ length: per_page }, (_, index) => {
    const stationIndex = ((page - 1) * per_page) + index

    if (search && stationIndex >= totalStations) {
      return null
    }

    const searchTerm = search.toLowerCase()

    // Generate consistent data for search
    let name = `Station ${stationIndex + 1}`
    let address = `${search ? 'Search ' : ''}123 Main Street, Tunis, Tunisia`

    if (search) {
      name = `${search} Result ${page}`
    }

    return {
      id: `STA-${stationIndex + 1}-${Date.now()}`,
      name,
      address,
      geometry: {
        type: 'Point',
        coordinates: [
          10.1815 + (Math.random() - 0.5) * 0.1,
          36.8065 + (Math.random() - 0.5) * 0.1,
        ],
      },
      amenities: ['WiFi', 'Parking'],
      operating_hours: '24/7',
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    }
  }).filter((station): station is Station => station !== null)

  return {
    data: filteredStations,
    meta: {
      page,
      per_page,
      total: totalStations,
      total_pages: totalPages,
    },
  }
}

export interface GeocodingResult {
  place_id: string
  lat: string
  lng: string
  display_name: string
  address: {
    road?: string
    city?: string
    state?: string
    country_code?: string
  }
}

export async function searchAddress(address: string): Promise<GeocodingResult[]> {
  // This simulates the OSM Nominatim geocoding API call
  // For now, return mock geocoding results

  await new Promise<void>(resolve => setTimeout(() => resolve(), 300))

  const searchTerm = address.toLowerCase()

  return [
    {
      place_id: `place-${Date.now()}`,
      lat: (36.8065 + Math.random() * 0.1).toFixed(6),
      lng: (10.1815 + Math.random() * 0.1).toFixed(6),
      display_name: `${address}, Tunis, Tunisia`,
      address: {
        road: 'Boulevard de la Liberté',
        city: 'Tunis',
        state: 'Tunisia',
        country_code: 'TN',
      },
    },
  ]
}

export async function searchStationByName(searchTerm: string): Promise<Station[]> {
  // Search for stations by name
  await new Promise<void>(resolve => setTimeout(() => resolve(), 300))

  const stations: Station[] = []
  const maxResults = 20

  for (let i = 1; i <= maxResults; i++) {
    stations.push({
      id: `STA-${i}-${Date.now()}`,
      name: `${searchTerm} Station ${i}`,
      address: '123 Main Street, Tunis, Tunisia',
      geometry: {
        type: 'Point',
        coordinates: [10.1815 + Math.random() * 0.1, 36.8065 + Math.random() * 0.1],
      },
      amenities: ['WiFi', 'Parking'],
      operating_hours: '24/7',
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    })
  }

  return stations
}

export interface FilterParams {
  chargerType?: 'CCS' | 'CHAdeMO' | 'AC'
  minPower?: number
  maxPower?: number
  available?: boolean
}

export async function filterStations(filters: FilterParams): Promise<Station[]> {
  // Filter stations based on criteria
  await new Promise<void>(resolve => setTimeout(() => resolve(), 200))

  return Array.from({ length: 10 }, (_, index) => ({
    id: `STA-${index + 1}-${Date.now()}`,
    name: `Filtered Station ${index + 1}`,
    address: '123 Main Street, Tunis, Tunisia',
    geometry: {
      type: 'Point',
      coordinates: [10.1815 + Math.random() * 0.1, 36.8065 + Math.random() * 0.1],
    },
    amenities: ['WiFi', 'Parking'],
    operating_hours: '24/7',
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  }))
}

export interface SortParams {
  sortBy?: 'distance' | 'name' | 'rating'
  sortOrder?: 'asc' | 'desc'
}

export async function sortStations(sortBy: SortParams): Promise<Station[]> {
  // Sort stations based on criteria
  await new Promise<void>(resolve => setTimeout(() => resolve(), 150))

  return Array.from({ length: 10 }, (_, index) => ({
    id: `STA-${index + 1}-${Date.now()}`,
    name: `Sorted Station ${index + 1}`,
    address: '123 Main Street, Tunis, Tunisia',
    geometry: {
      type: 'Point',
      coordinates: [10.1815 + Math.random() * 0.1, 36.8065 + Math.random() * 0.1],
    },
    amenities: ['WiFi', 'Parking'],
    operating_hours: '24/7',
    created_at: new Date().toISOString(),
    updated_at: new Date().toISOString(),
  }))
}
