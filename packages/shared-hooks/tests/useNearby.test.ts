import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook, waitFor, act } from '@testing-library/react';
import { useNearbyStations, useNearby } from '@bornemap/shared-hooks';
import type { Location } from '@bornemap/shared-types';

global.fetch = vi.fn();

const mockLocation: Location = { lat: 36.8, lon: 10.18 };

const mockResponse = {
  stations: [{
    id: 'sta_123', name: 'Test Station', visibility: 'commercial' as const,
    location: { lat: 36.8, lon: 10.18 }, distance_m: 2500, city: 'Tunis', status: 'active' as const,
  }],
  count: 1,
  radius_m: 5000,
};

beforeEach(() => { vi.clearAllMocks(); });
afterEach(() => { vi.restoreAllMocks(); });

describe('useNearby', () => {
  it('should return loading state initially', async () => {
    (global.fetch as any).mockResolvedValueOnce({ ok: false, status: 500, json: async () => ({ error: { code: 'INTERNAL_ERROR', message: 'Error' }, meta: {} }) });

    const { result } = renderHook(() => useNearby(mockLocation));
    expect(result.current.loading).toBe(true);
  });

  it('should return error state on API error', async () => {
    (global.fetch as any).mockResolvedValueOnce({ ok: false, status: 500, json: async () => ({ error: { code: 'INTERNAL_ERROR', message: 'Error' }, meta: {} }) });

    const { result } = renderHook(() => useNearby(mockLocation));
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.error).not.toBeNull();
  });

  it('should return stations on successful API call', async () => {
    (global.fetch as any).mockResolvedValueOnce({ ok: true, json: async () => mockResponse });

    const { result } = renderHook(() => useNearby(mockLocation));
    await waitFor(() => {
      expect(result.current.loading).toBe(false);
      expect(result.current.error).toBeNull();
    });
    expect(result.current.stations).not.toBeNull();
  });

  it('should handle network errors', async () => {
    (global.fetch as any).mockRejectedValueOnce(new Error('Network error'));

    const { result } = renderHook(() => useNearby(mockLocation));
    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.error).not.toBeNull();
  });

  it('should debounce requests', async () => {
    vi.useFakeTimers();
    (global.fetch as any).mockResolvedValue({ ok: true, json: async () => mockResponse });

    const { rerender } = renderHook(
      (loc: Location | null) => useNearby(loc, { debounceMs: 300 }),
      { initialProps: mockLocation }
    );

    // Immediately change location multiple times
    rerender({ lat: 37.0, lon: 10.5 });
    rerender({ lat: 37.2, lon: 10.8 });
    rerender({ lat: 37.4, lon: 11.0 });

    expect(global.fetch).toHaveBeenCalledTimes(0);

    act(() => { vi.advanceTimersByTime(300); });

    expect(global.fetch).toHaveBeenCalledTimes(1);

    vi.useRealTimers();
  });
});

describe('useNearbyStations', () => {
  it('should return loading state initially', async () => {
    (global.fetch as any).mockResolvedValueOnce({ ok: false, status: 500, json: async () => ({ error: { code: 'INTERNAL_ERROR', message: 'Error' }, meta: {} }) });

    const { result } = renderHook(() => useNearbyStations(mockLocation));
    expect(result.current.loading).toBe(true);
  });

  it('should return stations on successful API call', async () => {
    (global.fetch as any).mockResolvedValueOnce({ ok: true, json: async () => mockResponse });

    const { result } = renderHook(() => useNearbyStations(mockLocation));
    await waitFor(() => {
      expect(result.current.loading).toBe(false);
      expect(result.current.error).toBeNull();
    });
    expect(result.current.stations).toHaveLength(1);
    expect(result.current.count).toBe(1);
  });

  it('should handle empty results', async () => {
    (global.fetch as any).mockResolvedValueOnce({
      ok: true,
      json: async () => ({ stations: [], count: 0, radius_m: 5000 }),
    });

    const { result } = renderHook(() => useNearbyStations(mockLocation));
    await waitFor(() => {
      expect(result.current.stations).toHaveLength(0);
      expect(result.current.count).toBe(0);
    });
  });

  it('should allow custom parameters', async () => {
    (global.fetch as any).mockResolvedValueOnce({ ok: true, json: async () => mockResponse });

    const { result } = renderHook(() => useNearbyStations(mockLocation, { radius_m: 10000, max_results: 10, visibility: 'commercial' }));
    await waitFor(() => expect(result.current.loading).toBe(false));
  });

  it('should provide refetch function', async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => mockResponse });
    (global.fetch as any).mockImplementation(fetchMock);

    const { result } = renderHook(() => useNearbyStations(mockLocation));
    await waitFor(() => expect(result.current.loading).toBe(false));

    fetchMock.mockClear();
    act(() => { result.current.refetch({ lat: 37.0, lon: 10.5 }); });

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1), { timeout: 500 });
  });
});
