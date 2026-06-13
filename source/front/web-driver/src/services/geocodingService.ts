const NOMINATIM_BASE_URL = 'https://nominatim.openstreetmap.org'

export interface GeocodingResult {
  display_name: string
  lat: string
  lon: string
  place_id: string
}

export async function searchByAddress(query: string): Promise<GeocodingResult[]> {
  const params = new URLSearchParams({
    q: query,
    format: 'json',
    limit: '20',
    addressdetails: '1',
  })

  const response = await fetch(`${NOMINATIM_BASE_URL}/search?${params.toString()}`, {
    headers: {
      'User-Agent': 'Bornemap/1.0',
    },
    signal: AbortSignal.timeout(10000),
  })

  if (!response.ok) throw new Error('Geocoding request failed')
  return response.json()
}