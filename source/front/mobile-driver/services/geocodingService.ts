import { GeocodingResult } from './stationListService'

export interface NominatimGeocodingResponse {
  place_id: string
  license: string
  osm_type: string
  osm_id: string
  boundingbox: string[]
  lat: string
  lon: string
  display_name: string
  address: {
    road?: string
    house_number?: string
    postcode?: string
    city?: string
    state?: string
    country?: string
    country_code?: string
  }
  namedetails: {
    name: {
      default: string
    }
  }
}

export interface NominatimSearchOptions {
  q?: string
  addressdetails?: number
  limit?: number
  format?: 'json' | 'jsonv2'
  polygon_geojson?: number
}

export async function fetchGeocodingData(query: string, options: NominatimSearchOptions = {}): Promise<GeocodingResult[]> {
  const {
    q = query,
    addressdetails = 1,
    limit = 5,
    format = 'json',
    polygon_geojson = 0,
  } = options

  const baseUrl = 'https://nominatim.openstreetmap.org/search'

  try {
    const params = new URLSearchParams({
      q,
      addressdetails: addressdetails.toString(),
      limit: limit.toString(),
      format,
      polygon_geojson: polygon_geojson.toString(),
    })

    const response = await fetch(`${baseUrl}?${params}`, {
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'BorneMap/1.0 (bornemap@anomaly.co)',
      },
    })

    if (!response.ok) {
      throw new Error(`Nominatim API error: ${response.status}`)
    }

    const data: NominatimGeocodingResponse[] = await response.json()

    // Transform Nominatim response to our format
    return data.map((item) => ({
      place_id: item.place_id,
      lat: item.lat,
      lng: item.lon,
      display_name: item.display_name,
      address: {
        road: item.address.road,
        city: item.address.city,
        state: item.address.state,
        country_code: item.address.country_code,
      },
    }))
  } catch (error) {
    console.error('Nominatim geocoding failed:', error)
    throw error
  }
}

export async function searchByAddress(address: string): Promise<GeocodingResult[]> {
  try {
    const results = await fetchGeocodingData(address, {
      addressdetails: 1,
      limit: 5,
    })

    return results.map((item) => ({
      place_id: item.place_id,
      lat: item.lat,
      lng: item.lng,
      display_name: item.display_name,
      address: {
        road: item.address.road,
        city: item.address.city,
        state: item.address.state,
        country_code: item.address.country_code,
      },
    }))
  } catch (error) {
    console.error('Search by address failed:', error)
    return []
  }
}

export async function searchByCoordinates(lat: number, lng: number): Promise<GeocodingResult[]> {
  try {
    const results = await fetchGeocodingData(`${lat}, ${lng}`, {
      addressdetails: 1,
      limit: 5,
    })

    return results.map((item) => ({
      place_id: item.place_id,
      lat: item.lat,
      lng: item.lng,
      display_name: item.display_name,
      address: {
        road: item.address.road,
        city: item.address.city,
        state: item.address.state,
        country_code: item.address.country_code,
      },
    }))
  } catch (error) {
    console.error('Search by coordinates failed:', error)
    return []
  }
}

export async function reverseGeocode(lat: number, lng: number): Promise<GeocodingResult> {
  try {
    const results = await fetchGeocodingData(`${lat}, ${lng}`, {
      addressdetails: 1,
      limit: 1,
    })

    if (results.length === 0) {
      throw new Error('No results found for coordinates')
    }

    return {
      place_id: results[0].place_id,
      lat: results[0].lat,
      lng: results[0].lng,
      display_name: results[0].display_name,
      address: {
        road: results[0].address.road,
        city: results[0].address.city,
        state: results[0].address.state,
        country_code: results[0].address.country_code,
      },
    }
  } catch (error) {
    console.error('Reverse geocoding failed:', error)
    throw error
  }
}

// Retry configuration for rate limit handling
export interface RetryConfig {
  maxRetries: number
  initialDelay: number
  maxDelay: number
}

const DEFAULT_RETRY_CONFIG: RetryConfig = {
  maxRetries: 3,
  initialDelay: 1000,
  maxDelay: 60000,
}

export async function fetchGeocodingWithRetry(
  query: string,
  options: NominatimSearchOptions = {},
  retryConfig: RetryConfig = DEFAULT_RETRY_CONFIG,
): Promise<GeocodingResult[]> {
  let attempts = 0
  let lastError: Error | null = null

  while (attempts <= retryConfig.maxRetries) {
    try {
      return await fetchGeocodingData(query, options)
    } catch (error) {
      lastError = error as Error

      if (attempts < retryConfig.maxRetries) {
        const delay = Math.min(
          retryConfig.initialDelay * Math.pow(2, attempts),
          retryConfig.maxDelay,
        )
        console.log(`Nominatim API rate limited. Retrying in ${delay}ms...`)
        await new Promise<void>(resolve => setTimeout(() => resolve(), delay))
        attempts++
      } else {
        break
      }
    }
  }

  throw lastError || new Error('Failed to fetch geocoding data after retries')
}


