import { describe, it, expect } from "vitest";
import { fetchNearbyStations } from "../stationApi";
// @ts-expect-error - vitest vi not available during typecheck
const vi = global.vi || globalThis.vi;

describe("fetchNearbyStations", () => {
  it("correctly builds URL with required params", async () => {
    const mockFetch = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ data: [] }),
    });
    globalThis.fetch = mockFetch;

    await fetchNearbyStations({
      baseUrl: "http://localhost:3001",
      lat: 36.8,
      lon: 10.1,
    });

    const calledUrl = mockFetch.mock.calls[0][0];
    expect(calledUrl).toContain("lat=36.8");
    expect(calledUrl).toContain("lon=10.1");
  });
});
