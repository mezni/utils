import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { getNearby, getNearbyWithAuth } from '@bornemap/api-client';
import type { Location } from '@bornemap/shared-types';

global.fetch = vi.fn();

describe('API Client - getNearby', () => {
  const mockLocation: Location = { lat: 36.8, lon: 10.18 };

  const mockResponse = {
    stations: [
      {
        id: 'sta_123',
        name: 'Test Station',
        visibility: 'commercial',
        location: { lat: 36.8, lon: 10.18 },
        distance_m: 2500,
        city: 'Tunis',
        status: 'active',
      },
    ],
    count: 1,
    radius_m: 5000,
  };

  beforeEach(() => { vi.clearAllMocks(); });
  afterEach(() => { vi.restoreAllMocks(); });

  it('should make a successful GET request', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const result = await getNearby(mockLocation);

    expect(global.fetch).toHaveBeenCalledWith(
      expect.stringContaining('/api/v1/nearby?lat=36.8&lon=10.18')
    );
    expect(result).toEqual(mockResponse);
  });

  it('should use custom radius', async () => {
    (global.fetch as any).mockResolvedValueOnce({ ok: true, json: async () => mockResponse });
    await getNearby({ ...mockLocation, radius_m: 10000 });
    expect(global.fetch).toHaveBeenCalledWith(expect.stringContaining('radius_m=10000'), expect.any(Object));
  });

  it('should use custom max_results', async () => {
    (global.fetch as any).mockResolvedValueOnce({ ok: true, json: async () => mockResponse });
    await getNearby({ ...mockLocation, max_results: 10 });
    expect(global.fetch).toHaveBeenCalledWith(expect.stringContaining('max_results=10'), expect.any(Object));
  });

  it('should use custom visibility filter', async () => {
    (global.fetch as any).mockResolvedValueOnce({ ok: true, json: async () => mockResponse });
    await getNearby({ ...mockLocation, visibility: 'commercial' });
    expect(global.fetch).toHaveBeenCalledWith(expect.stringContaining('visibility=commercial'), expect.any(Object));
  });

  it('should return error response on 400', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: false, status: 400,
      json: async () => ({ error: { code: 'GEO_001', message: 'Invalid coordinates', field: 'coordinates' }, meta: { request_id: 'test-uuid', timestamp: new Date().toISOString() } }),
    });

    const result = await getNearby(mockLocation);
    expect(result).toHaveProperty('error');
    expect((result as any).error?.code).toBe('GEO_001');
  });

  it('should return error response on 401', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: false, status: 401,
      json: async () => ({ error: { code: 'AUTH_001', message: 'Unauthorized', field: 'authorization' }, meta: { request_id: 'test-uuid', timestamp: new Date().toISOString() } }),
    });

    const result = await getNearby(mockLocation);
    expect(result).toHaveProperty('error');
    expect((result as any).error?.code).toBe('AUTH_001');
  });

  it('should return error response on 500', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: false, status: 500,
      json: async () => ({ error: { code: 'INTERNAL_ERROR', message: 'Internal server error' }, meta: { request_id: 'test-uuid', timestamp: new Date().toISOString() } }),
    });

    const result = await getNearby(mockLocation);
    expect(result).toHaveProperty('error');
    expect((result as any).error?.code).toBe('INTERNAL_ERROR');
  });
});

describe('API Client - getNearbyWithAuth', () => {
  const mockLocation: Location = { lat: 36.8, lon: 10.18 };
  const mockToken = 'valid-jwt-token';

  const mockResponse = {
    stations: [{ id: 'sta_123', name: 'Test', visibility: 'commercial', location: { lat: 36.8, lon: 10.18 }, distance_m: 2500, city: 'Tunis', status: 'active' }],
    count: 1, radius_m: 5000,
  };

  beforeEach(() => { vi.clearAllMocks(); });
  afterEach(() => { vi.restoreAllMocks(); });

  it('should include auth token in request headers', async () => {
    (global.fetch as any).mockResolvedValueOnce({ ok: true, json: async () => mockResponse });

    await getNearbyWithAuth(mockLocation, mockToken);

    const [, init] = (global.fetch as any).mock.calls[0];
    expect(init.headers).toBeDefined();
    expect(init.headers['Authorization']).toBe('Bearer valid-jwt-token');
  });

  it('should handle 401 with invalid token', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: false, status: 401,
      json: async () => ({ error: { code: 'AUTH_001', message: 'Invalid token' }, meta: { request_id: 'test-uuid', timestamp: new Date().toISOString() } }),
    });

    const result = await getNearbyWithAuth(mockLocation, mockToken);
    expect(result).toHaveProperty('error');
    expect((result as any).error?.code).toBe('AUTH_001');
  });

  it('should return stations on successful auth request', async () => {
    (global.fetch as any).mockResolvedValueOnce({ ok: true, json: async () => mockResponse });

    const result = await getNearbyWithAuth(mockLocation, mockToken);
    expect(result).toEqual(mockResponse);
    expect(result).not.toHaveProperty('error');
  });
});
