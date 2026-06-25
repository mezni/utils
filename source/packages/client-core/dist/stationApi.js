import { NearbyResponseSchema, NearbyParamsSchema } from "@bornemap/domain-types";
export async function fetchNearbyStations(params) {
    const validatedParams = NearbyParamsSchema.parse({
        lat: params.lat,
        lon: params.lon,
        radius: params.radius,
        limit: params.limit,
    });
    const url = new URL(`${params.baseUrl}/api/v1/stations/nearby`);
    url.searchParams.set("lat", String(validatedParams.lat));
    url.searchParams.set("lon", String(validatedParams.lon));
    if (validatedParams.radius !== undefined) {
        url.searchParams.set("radius", String(validatedParams.radius));
    }
    if (validatedParams.limit !== undefined) {
        url.searchParams.set("limit", String(validatedParams.limit));
    }
    const res = await fetch(url.toString());
    if (!res.ok) {
        throw new Error(`API error: ${res.status} ${res.statusText}`);
    }
    const body = await res.json();
    const parsed = NearbyResponseSchema.parse(body);
    return parsed.data;
}
//# sourceMappingURL=stationApi.js.map