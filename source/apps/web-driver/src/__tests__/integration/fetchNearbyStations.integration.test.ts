import { describe, it, expect, vi, beforeEach } from "vitest";
import { fetchNearbyStations } from "../stationApi";

describe("Integration: fetchNearbyStations", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("should fetch nearby stations from correct endpoint", async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        data: [
          {
            station_id: "STA-test",
            name: "Test Station",
            lat: 36.8,
            lon: 10.1,
            distance_km: 1.5,
          },
        ],
      }),
    });
    globalThis.fetch = mockFetch;

    const result = await fetchNearbyStations({
      baseUrl: "http://localhost:3001",
      lat: 36.8,
      lon: 10.1,
    });

    expect(result).toHaveLength(1);
    expect(result[0].station_id).toBe("STA-test");
    expect(result[0].name).toBe("Test Station");
  });

  it("should handle API errors gracefully", async () => {
    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: false,
      status: 500,
      statusText: "Internal Server Error",
    });

    await expect(
      fetchNearbyStations({
        baseUrl: "http://localhost:3001",
        lat: 36.8,
        lon: 10.1,
      }),
    ).rejects.toThrow("API error: 500");
  });
});
