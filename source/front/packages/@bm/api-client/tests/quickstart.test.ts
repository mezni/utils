import { describe, it, expect } from 'vitest'
import { createApiClient, ApiError } from '../src/index'

// Quickstart.md validation — verifies the usage patterns documented there work
describe('quickstart validation', () => {
  it('createApiClient is a function', () => {
    expect(typeof createApiClient).toBe('function')
  })

  it('createApiClient returns an object with the expected methods', () => {
    const client = createApiClient('http://localhost:3000')
    expect(client).toHaveProperty('getStations')
    expect(client).toHaveProperty('getStationById')
    expect(client).toHaveProperty('getNearbyStations')
    expect(typeof client.getStations).toBe('function')
    expect(typeof client.getStationById).toBe('function')
    expect(typeof client.getNearbyStations).toBe('function')
  })

  it('ApiError is a class extending Error', () => {
    const err = new ApiError(500, 'Internal Server Error')
    expect(err).toBeInstanceOf(Error)
    expect(err.name).toBe('ApiError')
    expect(err.status).toBe(500)
    expect(err.message).toBe('Internal Server Error')
  })

  it('ApiError can be caught with instanceof', () => {
    try {
      throw new ApiError(404, 'Not Found')
    } catch (e) {
      expect(e instanceof ApiError).toBe(true)
      if (e instanceof ApiError) {
        expect(e.status).toBe(404)
      }
    }
  })
})
