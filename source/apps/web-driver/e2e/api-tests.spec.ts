import { test, expect } from "@playwright/test";

test.describe("Driver-Service API (Sprint 02)", () => {
  test.beforeEach(async ({ request }) => {
    // Test against the API
  });

  test("GET /api/v1/health returns 200 and ok status", async ({ request }) => {
    const response = await request.get("/api/v1/health");
    expect(response.status()).toBe(200);
    const json = await response.json();
    expect(json).toEqual({
      status: "ok",
      service: "driver-service",
      version: "1.0.0",
    });
  });

  test("GET /api/v1/stations/nearby with valid params returns 200", async ({
    request,
  }) => {
    const response = await request.get("/api/v1/stations/nearby", {
      params: {
        lat: 36.8,
        lon: 10.1,
        radius: 5000,
        limit: 50,
      },
    });
    expect(response.status()).toBe(200);
    const json = await response.json();
    expect(json).toHaveProperty("data");
    expect(Array.isArray(json.data)).toBe(true);
    if (json.data.length > 0) {
      const station = json.data[0];
      expect(station).toHaveProperty("station_id");
      expect(station).toHaveProperty("name");
      expect(station).toHaveProperty("lat");
      expect(station).toHaveProperty("lon");
      expect(station).toHaveProperty("distance_km");
    }
  });

  test("GET /api/v1/stations/nearby with invalid lat returns 400", async ({
    request,
  }) => {
    const response = await request.get("/api/v1/stations/nearby", {
      params: { lat: 999, lon: 10.1 },
    });
    expect(response.status()).toBe(400);
    const json = await response.json();
    expect(json).toHaveProperty("error");
  });

  test("GET /api/v1/stations/nearby with missing lat returns 400", async ({
    request,
  }) => {
    const response = await request.get("/api/v1/stations/nearby", {
      params: { lon: 10.1 },
    });
    expect(response.status()).toBe(400);
    const json = await response.json();
    expect(json).toHaveProperty("error");
  });

  test("GET /api/v1/stations/nearby with radius=0 returns 400", async ({
    request,
  }) => {
    const response = await request.get("/api/v1/stations/nearby", {
      params: { lat: 36.8, lon: 10.1, radius: 0 },
    });
    expect(response.status()).toBe(400);
    const json = await response.json();
    expect(json).toHaveProperty("error");
  });

  test("GET /api/v1/stations/nearby with limit=0 returns 400", async ({
    request,
  }) => {
    const response = await request.get("/api/v1/stations/nearby", {
      params: { lat: 36.8, lon: 10.1, limit: 0 },
    });
    expect(response.status()).toBe(400);
    const json = await response.json();
    expect(json).toHaveProperty("error");
  });
});
