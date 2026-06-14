import type { Station } from '@bm/types'
import { ApiError } from './errors'
import { createTransport, type Transport } from './transport'
import {
  toStation,
  validateLat,
  validateLng,
  validateRadius,
  validateId,
} from './types'

export interface ApiClient {
  getStations(): Promise<Station[]>
  getStationById(id: string): Promise<Station>
  getNearbyStations(lat: number, lng: number, radius: number): Promise<Station[]>
}

export function createApiClient(baseUrl: string): ApiClient {
  const transport: Transport = createTransport(baseUrl)

  async function handleResponse<T>(response: Response): Promise<T> {
    if (!response.ok) {
      const body = await response.json().catch(() => null)
      throw new ApiError(response.status, response.statusText, body)
    }
    return response.json() as Promise<T>
  }

  return {
    async getStations(): Promise<Station[]> {
      const response = await transport.request('/api/v1/stations')
      const raw = await handleResponse<any[]>(response)
      return raw.map(toStation)
    },

    async getStationById(id: string): Promise<Station> {
      validateId(id)
      const response = await transport.request(`/api/v1/stations/${encodeURIComponent(id)}`)
      const raw = await handleResponse<any>(response)
      return toStation(raw)
    },

    async getNearbyStations(lat: number, lng: number, radius: number): Promise<Station[]> {
      validateLat(lat)
      validateLng(lng)
      validateRadius(radius)
      const params = new URLSearchParams({ lat: String(lat), lng: String(lng), radius: String(radius) })
      const response = await transport.request(`/api/v1/stations/nearby?${params}`)
      const raw = await handleResponse<any[]>(response)
      return raw.map(toStation)
    },
  }
}
