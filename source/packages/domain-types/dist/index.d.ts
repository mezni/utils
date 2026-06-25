import { z } from "zod";
export declare const StationSchema: z.ZodObject<{
    station_id: z.ZodString;
    name: z.ZodNullable<z.ZodString>;
    lat: z.ZodNumber;
    lon: z.ZodNumber;
    distance_km: z.ZodNumber;
}, "strip", z.ZodTypeAny, {
    station_id: string;
    name: string | null;
    lat: number;
    lon: number;
    distance_km: number;
}, {
    station_id: string;
    name: string | null;
    lat: number;
    lon: number;
    distance_km: number;
}>;
export type StationDto = z.infer<typeof StationSchema>;
export declare const NearbyResponseSchema: z.ZodObject<{
    data: z.ZodArray<z.ZodObject<{
        station_id: z.ZodString;
        name: z.ZodNullable<z.ZodString>;
        lat: z.ZodNumber;
        lon: z.ZodNumber;
        distance_km: z.ZodNumber;
    }, "strip", z.ZodTypeAny, {
        station_id: string;
        name: string | null;
        lat: number;
        lon: number;
        distance_km: number;
    }, {
        station_id: string;
        name: string | null;
        lat: number;
        lon: number;
        distance_km: number;
    }>, "many">;
}, "strip", z.ZodTypeAny, {
    data: {
        station_id: string;
        name: string | null;
        lat: number;
        lon: number;
        distance_km: number;
    }[];
}, {
    data: {
        station_id: string;
        name: string | null;
        lat: number;
        lon: number;
        distance_km: number;
    }[];
}>;
export type NearbyResponse = z.infer<typeof NearbyResponseSchema>;
export declare const NearbyParamsSchema: z.ZodObject<{
    lat: z.ZodNumber;
    lon: z.ZodNumber;
    radius: z.ZodOptional<z.ZodNumber>;
    limit: z.ZodOptional<z.ZodNumber>;
}, "strip", z.ZodTypeAny, {
    lat: number;
    lon: number;
    radius?: number | undefined;
    limit?: number | undefined;
}, {
    lat: number;
    lon: number;
    radius?: number | undefined;
    limit?: number | undefined;
}>;
export type NearbyParams = z.infer<typeof NearbyParamsSchema>;
//# sourceMappingURL=index.d.ts.map