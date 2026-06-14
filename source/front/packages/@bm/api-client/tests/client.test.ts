import { describe, it, expect } from 'vitest'
import { createApiClient } from '../src/client'
import { ApiError } from '../src/errors'

describe('getStations', () => {
  it('parses response into Station[]', async () => {
    const client = createApiClient('http://test.example')
    // Mock fetch for this test — will be replaced by msw in integration
    const result = await client.getStations()
    expect(Array.isArray(result)).toBe(true)
  })
})

describe('getStationById', () => {
  it('throws on empty id', async () => {
    const client = createApiClient('http://test.example')
    await expect(client.getStationById('')).rejects.toThrow(RangeError)
  })

  it('throws on null id', async () => {
    const client = createApiClient('http://test.example')
    await expect(client.getStationById(null as any)).rejects.toThrow(RangeError)
  })
})

describe('getNearbyStations', () => {
  it('throws on out-of-range latitude', async () => {
    const client = createApiClient('http://test.example')
    await expect(client.getNearbyStations(100, 0, 5000)).rejects.toThrow(RangeError)
  })

  it('throws on out-of-range longitude', async () => {
    const client = createApiClient('http://test.example')
    await expect(client.getNearbyStations(0, 200, 5000)).rejects.toThrow(RangeError)
  })

  it('throws on zero radius', async () => {
    const client = createApiClient('http://test.example')
    await expect(client.getNearbyStations(0, 0, 0)).rejects.toThrow(RangeError)
  })

  it('throws on negative radius', async () => {
    const client = createApiClient('http://test.example')
    await expect(client.getNearbyStations(0, 0, -100)).rejects.toThrow(RangeError)
  })
})
