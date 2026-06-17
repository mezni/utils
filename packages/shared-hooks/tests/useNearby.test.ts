import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, waitFor } from '@testing-library/react';
import { useNearbyStations, useNearby } from '@bornemap/shared-hooks';
import type { Location } from '@bornemap/shared-types';

// Mock fetch
global.fetch = vi.fn();

describe('useNearby', () => {
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

  it('should return loading state initially', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: false,
      status: 500,
      json: async () => ({
        error: {
          code: 'INTERNAL_ERROR',
          message: 'Internal server error',
        },
        meta: {
          request_id: 'test-uuid',
          timestamp: new Date().toISOString(),
        },
      }),
    });

    const { result } = renderHook(() => useNearby(mockLocation));

    await waitFor(() => {
      expect(result.current.loading).toBe(true);
    });
  });

  it('should return error state on API error', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: false,
      status: 500,
      json: async () => ({
        error: {
          code: 'INTERNAL_ERROR',
          message: 'Internal server error',
        },
        meta: {
          request_id: 'test-uuid',
          timestamp: new Date().toISOString(),
        },
      }),
    });

    const { result } = renderHook(() => useNearby(mockLocation));

    await waitFor(() => {
      expect(result.current.loading).toBe(false);
      expect(result.current.error).not.toBeNull();
    });
  });

  it('should return stations on successful API call', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const { result } = renderHook(() => useNearby(mockLocation));

    await waitFor(() => {
      expect(result.current.loading).toBe(false);
      expect(result.current.error).toBeNull();
      expect(result.current.stations).not.toBeNull();
    });
  });

  it('should handle network errors', async () => {
    (global.fetch as any).mockRejectedValueOnce(new Error('Network error'));

    const { result } = renderHook(() => useNearby(mockLocation));

    await waitFor(() => {
      expect(result.current.loading).toBe(false);
      expect(result.current.error).not.toBeNull();
    });
  });

  it('should debounce requests by default (300ms)', async () => {
    const fetchMock = vi.fn();
    (global.fetch as any).mockImplementation(fetchMock);

    const { rerender } = renderHook(() => useNearby(mockLocation));

    // Rapidly change location
    rerender({ location: { lat: 37.0, lon: 10.5 } } as any);
    rerender({ location: { lat: 37.2, lon: 10.8 } } as any);
    rerender({ location: { lat: 37.4, lon: 11.0 } } as any);

    // First fetch should happen after debounce
    await waitFor(
      () => {
        expect(fetchMock).toHaveBeenCalledTimes(1);
      },
      { timeout: 400 }
    );
  });

  it('should allow custom debounce duration', async () => {
    const fetchMock = vi.fn();
    (global.fetch as any).mockImplementation(fetchMock);

    const { rerender } = renderHook(
      () => useNearby(mockLocation, { debounceMs: 500 }),
      {
        wrapper: ({ children }) => <div>{children}</div>,
      }
    );

    // Rapidly change location
    rerender({ location: { lat: 37.0, lon: 10.5 } } as any);
    rerender({ location: { lat: 37.2, lon: 10.8 } } as any);
    rerender({ location: { lat: 37.4, lon: 11.0 } } as any);

    // First fetch should happen after 500ms
    await waitFor(
      () => {
        expect(fetchMock).toHaveBeenCalledTimes(1);
      },
      { timeout: 600 }
    );
  });
});

describe('useNearbyStations', () => {
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

  it('should return loading state initially', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: false,
      status: 500,
      json: async () => ({
        error: {
          code: 'INTERNAL_ERROR',
          message: 'Internal server error',
        },
        meta: {
          request_id: 'test-uuid',
          timestamp: new Date().toISOString(),
        },
      }),
    });

    const { result } = renderHook(() => useNearbyStations(mockLocation));

    await waitFor(() => {
      expect(result.current.loading).toBe(true);
    });
  });

  it('should return error state on API error', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: false,
      status: 400,
      json: async () => ({
        error: {
          code: 'GEO_001',
          message: 'Invalid coordinates',
        },
        meta: {
          request_id: 'test-uuid',
          timestamp: new Date().toISOString(),
        },
      }),
    });

    const { result } = renderHook(() => useNearbyStations(mockLocation));

    await waitFor(() => {
      expect(result.current.loading).toBe(false);
      expect(result.current.error).not.toBeNull();
    });
  });

  it('should return stations on successful API call', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const { result } = renderHook(() => useNearbyStations(mockLocation));

    await waitFor(() => {
      expect(result.current.loading).toBe(false);
      expect(result.current.error).toBeNull();
      expect(result.current.stations.length).toBe(1);
      expect(result.current.count).toBe(1);
    });
  });

  it('should handle empty results', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: true,
      json: async () => ({
        data: {
          stations: [],
          count: 0,
          radius_m: 5000,
        },
        meta: {
          request_id: 'test-uuid',
          timestamp: new Date().toISOString(),
        },
      }),
    });

    const { result } = renderHook(() => useNearbyStations(mockLocation));

    await waitFor(() => {
      expect(result.current.stations.length).toBe(0);
      expect(result.current.count).toBe(0);
    });
  });

  it('should allow custom parameters', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: true,
      json: async () => mockResponse,
    });

    const { result } = renderHook(() =>
      useNearbyStations(mockLocation, {
        radius_m: 10000,
        max_results: 10,
        visibility: 'commercial',
      })
    );

    await waitFor(() => {
      expect(result.current.loading).toBe(false);
    });
  });

  it('should provide refetch function', async () => {
    const fetchMock = vi.fn();
    (global.fetch as any).mockImplementation(fetchMock);

    const { result } = renderHook(() => useNearbyStations(mockLocation));

    await waitFor(() => {
      expect(result.current.loading).toBe(false);
    });

    // Call refetch
    result.current.refetch(mockLocation);

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledTimes(2);
    });
  });
});
