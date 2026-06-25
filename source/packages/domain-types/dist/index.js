import { z } from "zod";
export const StationSchema = z.object({
    station_id: z.string(),
    name: z.string().nullable(),
    lat: z.number(),
    lon: z.number(),
    distance_km: z.number(),
});
export const NearbyResponseSchema = z.object({
    data: z.array(StationSchema),
});
export const NearbyParamsSchema = z.object({
    lat: z.number().min(-90).max(90),
    lon: z.number().min(-180).max(180),
    radius: z.number().int().positive().optional(),
    limit: z.number().int().min(1).max(100).optional(),
});
//# sourceMappingURL=index.js.map