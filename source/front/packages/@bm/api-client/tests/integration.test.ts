import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { http, HttpResponse } from 'msw'
import { setupServer } from 'msw/node'
import { createApiClient } from '../src/client'

const handlers = [
  http.get('http://test.example/api/v1/stations', () => {
    return HttpResponse.json([
      {
        id: 'STA-001',
        name: 'Station A',
        status: 'active',
        latitude: 36.8,
        longitude: 10.18,
        location: { type: 'Point', coordinates: [10.18, 36.8] },
        distance: 100,
      },
      {
        id: 'STA-002',
        name: 'Station B',
        status: 'maintenance',
        latitude: 36.85,
        longitude: 10.2,
        location: { type: 'Point', coordinates: [10.2, 36.85] },
        distance: 200,
      },
    ])
  }),

  http.get('http://test.example/api/v1/stations/STA-001', () => {
    return HttpResponse.json({
      id: 'STA-001',
      name: 'Station A',
      status: 'active',
      latitude: 36.8,
      longitude: 10.18,
      location: { type: 'Point', coordinates: [10.18, 36.8] },
      distance: 100,
    })
  }),

  http.get('http://test.example/api/v1/stations/NONEXISTENT', () => {
    return new HttpResponse(null, { status: 404, statusText: 'Not Found' })
  }),

  http.get('http://test.example/api/v1/stations/nearby', ({ request }) => {
    const url = new URL(request.url)
    const lat = parseFloat(url.searchParams.get('lat')!)
    const lng = parseFloat(url.searchParams.get('lng')!)
    const radius = parseFloat(url.searchParams.get('radius')!)

    if (isNaN(lat) || isNaN(lng) || isNaN(radius)) {
      return new HttpResponse(null, { status: 400, statusText: 'Bad Request' })
    }

    return HttpResponse.json([
      {
        id: 'STA-001',
        name: 'Station A',
        status: 'active',
        latitude: lat + 0.01,
        longitude: lng + 0.01,
        location: { type: 'Point', coordinates: [lng + 0.01, lat + 0.01] },
        distance: 500,
      },
    ])
  }),
]

const server = setupServer(...handlers)

beforeAll(() => server.listen({ onUnhandledRequest: 'error' }))
afterAll(() => server.close())

describe('getStations integration', () => {
  it('returns typed Station[] from API', async () => {
    const client = createApiClient('http://test.example')
    const stations = await client.getStations()

    expect(stations).toHaveLength(2)
    expect(stations[0]).toMatchObject({
      id: 'STA-001',
      name: 'Station A',
      status: 'active',
    })
  })
})

describe('getStationById integration', () => {
  it('returns a single station', async () => {
    const client = createApiClient('http://test.example')
    const station = await client.getStationById('STA-001')

    expect(station).toMatchObject({
      id: 'STA-001',
      name: 'Station A',
    })
  })

  it('throws ApiError on 404', async () => {
    const client = createApiClient('http://test.example')
    await expect(client.getStationById('NONEXISTENT')).rejects.toThrow()
  })
})

describe('getNearbyStations integration', () => {
  it('returns stations within radius', async () => {
    const client = createApiClient('http://test.example')
    const stations = await client.getNearbyStations(36.8, 10.18, 5000)

    expect(stations).toHaveLength(1)
    expect(stations[0].id).toBe('STA-001')
  })
})
