import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { getNearby, getNearbyWithAuth } from '@bornemap/api-client';
import type { Location } from '@bornemap/shared-types';

// Mock fetch
global.fetch = vi.fn();

describe('API Client - getNearby', () => {
  const mockLocation: Location = {
    lat: 36.8,
    lon: 10.18,
  };

  const mockResponse = {
    data: {
      stations: [
        {
          id: 'sta_123',
          name: 'Test Station',
          visibility: 'commercial',
          location: { lat: 36.8, lon: 10.18 },
          distance_km: 2.5,
          city: 'Tunis',
          status: 'active',
        },
      ],
      count: 1,
      radius_m: 5000,
    },
    meta: {
      request_id: 'test-uuid',
      timestamp: new Date().toISOString(),
    },
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should make a successful GET request', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const result = await getNearby(mockLocation);

    expect(global.fetch).toHaveBeenCalledWith(
      'http://localhost:3001/api/v1/nearby?lat=36.8&lon=10.18&radius_m=5000&max_results=50&visibility=all'
    );
    expect(result).toEqual(mockResponse);
  });

  it('should use custom radius', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    await getNearby({
      ...mockLocation,
      radius_m: 10000,
    });

    expect(global.fetch).toHaveBeenCalledWith(
      expect.stringContaining('radius_m=10000')
    );
  });

  it('should use custom max_results', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    await getNearby({
      ...mockLocation,
      max_results: 10,
    });

    expect(global.fetch).toHaveBeenCalledWith(
      expect.stringContaining('max_results=10')
    );
  });

  it('should use custom visibility filter', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    await getNearby({
      ...mockLocation,
      visibility: 'commercial',
    });

    expect(global.fetch).toHaveBeenCalledWith(
      expect.stringContaining('visibility=commercial')
    );
  });

  it('should return error response on 400', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: false,
      status: 400,
      json: async () => ({
        error: {
          code: 'GEO_001',
          message: 'Invalid coordinates',
          field: 'coordinates',
        },
        meta: {
          request_id: 'test-uuid',
          timestamp: new Date().toISOString(),
        },
      }),
    });

    const result = await getNearby(mockLocation);

    expect(result).toHaveProperty('error');
    expect(result.error?.code).toBe('GEO_001');
    expect(result.error?.message).toBe('Invalid coordinates');
  });

  it('should return error response on 401', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: false,
      status: 401,
      json: async () => ({
        error: {
          code: 'AUTH_001',
          message: 'Unauthorized',
          field: 'authorization',
        },
        meta: {
          request_id: 'test-uuid',
          timestamp: new Date().toISOString(),
        },
      }),
    });

    const result = await getNearby(mockLocation);

    expect(result).toHaveProperty('error');
    expect(result.error?.code).toBe('AUTH_001');
    expect(result.error?.message).toBe('Unauthorized');
  });

  it('should return error response on 500', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: false,
      status: 500,
      json: async () => ({
        error: {
          code: 'INTERNAL_ERROR',
          message: 'Internal server error',
          field: null,
        },
        meta: {
          request_id: 'test-uuid',
          timestamp: new Date().toISOString(),
        },
      }),
    });

    const result = await getNearby(mockLocation);

    expect(result).toHaveProperty('error');
    expect(result.error?.code).toBe('INTERNAL_ERROR');
    expect(result.error?.message).toBe('Internal server error');
  });
});

describe('API Client - getNearbyWithAuth', () => {
  const mockLocation: Location = {
    lat: 36.8,
    lon: 10.18,
  };

  const mockToken = 'valid-jwt-token';

  const mockResponse = {
    data: {
      stations: [
        {
          id: 'sta_123',
          name: 'Test Station',
          visibility: 'commercial',
          location: { lat: 36.8, lon: 10.18 },
          distance_km: 2.5,
          city: 'Tunis',
          status: 'active',
        },
      ],
      count: 1,
      radius_m: 5000,
    },
    meta: {
      request_id: 'test-uuid',
      timestamp: new Date().toISOString(),
    },
  };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should include auth token in request', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    await getNearbyWithAuth(mockLocation, mockToken);

    expect(global.fetch).toHaveBeenCalledWith(
      expect.stringContaining('Authorization: Bearer valid-jwt-token')
    );
  });

  it('should handle 401 with invalid token', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: false,
      status: 401,
      json: async () => ({
        error: {
          code: 'AUTH_001',
          message: 'Invalid token',
          field: 'authorization',
        },
        meta: {
          request_id: 'test-uuid',
          timestamp: new Date().toISOString(),
        },
      }),
    });

    const result = await getNearbyWithAuth(mockLocation, mockToken);

    expect(result).toHaveProperty('error');
    expect(result.error?.code).toBe('AUTH_001');
  });

  it('should return stations on successful auth request', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const result = await getNearbyWithAuth(mockLocation, mockToken);

    expect(result).toEqual(mockResponse);
    expect(result).not.toHaveProperty('error');
  });
});
