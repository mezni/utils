export interface Station {
  id: string
  name: string
  address: string
  amenities: string[]
  geometry: {
    type: 'Point'
    coordinates: [number, number]
  }
  distance_km?: number
}

export interface StationListResponse {
  data: Station[]
  meta: {
    current_page: number
    total_pages: number
    total: number
  }
}

export async function fetchStations(params: {
  page?: number
  per_page?: number
  lat?: number
  lng?: number
  radius?: number
}): Promise<StationListResponse> {
  const searchParams = new URLSearchParams()
  if (params.page) searchParams.set('page', params.page.toString())
  if (params.per_page) searchParams.set('per_page', params.per_page.toString())
  if (params.lat) searchParams.set('lat', params.lat.toString())
  if (params.lng) searchParams.set('lng', params.lng.toString())
  if (params.radius) searchParams.set('radius', params.radius.toString())

  const response = await fetch(`/api/stations?${searchParams.toString()}`)
  if (!response.ok) throw new Error('Failed to fetch stations')
  return response.json()
}