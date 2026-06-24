import { z } from "zod";

export const StationSchema = z.object({
  station_id: z.string(),
  name: z.string().nullable(),
  lat: z.number(),
  lon: z.number(),
  distance_km: z.number(),
});

export type StationDto = z.infer<typeof StationSchema>;

export const NearbyResponseSchema = z.object({
  data: z.array(StationSchema),
});

export type NearbyResponse = z.infer<typeof NearbyResponseSchema>;

export const NearbyParamsSchema = z.object({
  lat: z.number().min(-90).max(90),
  lon: z.number().min(-180).max(180),
  radius: z.number().int().positive().optional(),
  limit: z.number().int().min(1).max(100).optional(),
});

export type NearbyParams = z.infer<typeof NearbyParamsSchema>;
