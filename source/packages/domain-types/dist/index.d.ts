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
export declare const AdminPartnerSchema: z.ZodObject<{
    partner_id: z.ZodString;
    name: z.ZodString;
    partner_type: z.ZodNullable<z.ZodString>;
    support_phone: z.ZodNullable<z.ZodString>;
    support_email: z.ZodNullable<z.ZodString>;
    is_verified: z.ZodBoolean;
    created_at: z.ZodString;
    updated_at: z.ZodNullable<z.ZodString>;
}, "strip", z.ZodTypeAny, {
    name: string;
    partner_id: string;
    partner_type: string | null;
    support_phone: string | null;
    support_email: string | null;
    is_verified: boolean;
    created_at: string;
    updated_at: string | null;
}, {
    name: string;
    partner_id: string;
    partner_type: string | null;
    support_phone: string | null;
    support_email: string | null;
    is_verified: boolean;
    created_at: string;
    updated_at: string | null;
}>;
export type AdminPartnerDto = z.infer<typeof AdminPartnerSchema>;
export declare const AdminStationSchema: z.ZodObject<{
    station_id: z.ZodString;
    osm_id: z.ZodNullable<z.ZodNumber>;
    partner_id: z.ZodNullable<z.ZodString>;
    name: z.ZodString;
    address: z.ZodNullable<z.ZodString>;
    lat: z.ZodNumber;
    lon: z.ZodNumber;
    created_at: z.ZodString;
    updated_at: z.ZodNullable<z.ZodString>;
}, "strip", z.ZodTypeAny, {
    station_id: string;
    name: string;
    lat: number;
    lon: number;
    partner_id: string | null;
    created_at: string;
    updated_at: string | null;
    osm_id: number | null;
    address: string | null;
}, {
    station_id: string;
    name: string;
    lat: number;
    lon: number;
    partner_id: string | null;
    created_at: string;
    updated_at: string | null;
    osm_id: number | null;
    address: string | null;
}>;
export type AdminStationDto = z.infer<typeof AdminStationSchema>;
export declare const AdminChargerSchema: z.ZodObject<{
    charger_id: z.ZodString;
    station_id: z.ZodString;
    connector_type_id: z.ZodNumber;
    status_id: z.ZodNumber;
    current_type_id: z.ZodNumber;
    power_kw: z.ZodNullable<z.ZodNumber>;
    voltage: z.ZodNullable<z.ZodNumber>;
    amperage: z.ZodNullable<z.ZodNumber>;
    count_available: z.ZodNumber;
    count_total: z.ZodNumber;
    created_at: z.ZodString;
    updated_at: z.ZodNullable<z.ZodString>;
}, "strip", z.ZodTypeAny, {
    station_id: string;
    created_at: string;
    updated_at: string | null;
    charger_id: string;
    connector_type_id: number;
    status_id: number;
    current_type_id: number;
    power_kw: number | null;
    voltage: number | null;
    amperage: number | null;
    count_available: number;
    count_total: number;
}, {
    station_id: string;
    created_at: string;
    updated_at: string | null;
    charger_id: string;
    connector_type_id: number;
    status_id: number;
    current_type_id: number;
    power_kw: number | null;
    voltage: number | null;
    amperage: number | null;
    count_available: number;
    count_total: number;
}>;
export type AdminChargerDto = z.infer<typeof AdminChargerSchema>;
export declare const PaginationSchema: z.ZodObject<{
    page: z.ZodNumber;
    per_page: z.ZodNumber;
    total: z.ZodNumber;
    total_pages: z.ZodNumber;
}, "strip", z.ZodTypeAny, {
    page: number;
    per_page: number;
    total: number;
    total_pages: number;
}, {
    page: number;
    per_page: number;
    total: number;
    total_pages: number;
}>;
export type PaginationDto = z.infer<typeof PaginationSchema>;
export declare const PaginatedResponseSchema: <T extends z.ZodTypeAny>(item: T) => z.ZodObject<{
    data: z.ZodArray<T, "many">;
    pagination: z.ZodObject<{
        page: z.ZodNumber;
        per_page: z.ZodNumber;
        total: z.ZodNumber;
        total_pages: z.ZodNumber;
    }, "strip", z.ZodTypeAny, {
        page: number;
        per_page: number;
        total: number;
        total_pages: number;
    }, {
        page: number;
        per_page: number;
        total: number;
        total_pages: number;
    }>;
}, "strip", z.ZodTypeAny, {
    data: T["_output"][];
    pagination: {
        page: number;
        per_page: number;
        total: number;
        total_pages: number;
    };
}, {
    data: T["_input"][];
    pagination: {
        page: number;
        per_page: number;
        total: number;
        total_pages: number;
    };
}>;
export declare const CreatePartnerRequestSchema: z.ZodObject<{
    name: z.ZodString;
    partner_type: z.ZodOptional<z.ZodString>;
    support_phone: z.ZodOptional<z.ZodString>;
    support_email: z.ZodOptional<z.ZodString>;
}, "strip", z.ZodTypeAny, {
    name: string;
    partner_type?: string | undefined;
    support_phone?: string | undefined;
    support_email?: string | undefined;
}, {
    name: string;
    partner_type?: string | undefined;
    support_phone?: string | undefined;
    support_email?: string | undefined;
}>;
export type CreatePartnerRequest = z.infer<typeof CreatePartnerRequestSchema>;
export declare const UpdatePartnerRequestSchema: z.ZodObject<{
    name: z.ZodOptional<z.ZodString>;
    partner_type: z.ZodOptional<z.ZodString>;
    support_phone: z.ZodOptional<z.ZodString>;
    support_email: z.ZodOptional<z.ZodString>;
}, "strip", z.ZodTypeAny, {
    name?: string | undefined;
    partner_type?: string | undefined;
    support_phone?: string | undefined;
    support_email?: string | undefined;
}, {
    name?: string | undefined;
    partner_type?: string | undefined;
    support_phone?: string | undefined;
    support_email?: string | undefined;
}>;
export type UpdatePartnerRequest = z.infer<typeof UpdatePartnerRequestSchema>;
export declare const CreateStationRequestSchema: z.ZodObject<{
    name: z.ZodString;
    lat: z.ZodNumber;
    lon: z.ZodNumber;
    osm_id: z.ZodOptional<z.ZodNumber>;
    partner_id: z.ZodOptional<z.ZodString>;
    address: z.ZodOptional<z.ZodString>;
}, "strip", z.ZodTypeAny, {
    name: string;
    lat: number;
    lon: number;
    partner_id?: string | undefined;
    osm_id?: number | undefined;
    address?: string | undefined;
}, {
    name: string;
    lat: number;
    lon: number;
    partner_id?: string | undefined;
    osm_id?: number | undefined;
    address?: string | undefined;
}>;
export type CreateStationRequest = z.infer<typeof CreateStationRequestSchema>;
export declare const UpdateStationRequestSchema: z.ZodObject<{
    name: z.ZodOptional<z.ZodString>;
    address: z.ZodOptional<z.ZodString>;
    lat: z.ZodOptional<z.ZodNumber>;
    lon: z.ZodOptional<z.ZodNumber>;
    partner_id: z.ZodOptional<z.ZodString>;
}, "strip", z.ZodTypeAny, {
    name?: string | undefined;
    lat?: number | undefined;
    lon?: number | undefined;
    partner_id?: string | undefined;
    address?: string | undefined;
}, {
    name?: string | undefined;
    lat?: number | undefined;
    lon?: number | undefined;
    partner_id?: string | undefined;
    address?: string | undefined;
}>;
export type UpdateStationRequest = z.infer<typeof UpdateStationRequestSchema>;
export declare const CreateChargerRequestSchema: z.ZodObject<{
    station_id: z.ZodString;
    connector_type_id: z.ZodNumber;
    status_id: z.ZodNumber;
    current_type_id: z.ZodNumber;
    power_kw: z.ZodOptional<z.ZodNumber>;
    voltage: z.ZodOptional<z.ZodNumber>;
    amperage: z.ZodOptional<z.ZodNumber>;
    count_available: z.ZodOptional<z.ZodNumber>;
    count_total: z.ZodOptional<z.ZodNumber>;
}, "strip", z.ZodTypeAny, {
    station_id: string;
    connector_type_id: number;
    status_id: number;
    current_type_id: number;
    power_kw?: number | undefined;
    voltage?: number | undefined;
    amperage?: number | undefined;
    count_available?: number | undefined;
    count_total?: number | undefined;
}, {
    station_id: string;
    connector_type_id: number;
    status_id: number;
    current_type_id: number;
    power_kw?: number | undefined;
    voltage?: number | undefined;
    amperage?: number | undefined;
    count_available?: number | undefined;
    count_total?: number | undefined;
}>;
export type CreateChargerRequest = z.infer<typeof CreateChargerRequestSchema>;
export declare const UpdateChargerRequestSchema: z.ZodObject<{
    connector_type_id: z.ZodOptional<z.ZodNumber>;
    status_id: z.ZodOptional<z.ZodNumber>;
    current_type_id: z.ZodOptional<z.ZodNumber>;
    power_kw: z.ZodOptional<z.ZodNumber>;
    voltage: z.ZodOptional<z.ZodNumber>;
    amperage: z.ZodOptional<z.ZodNumber>;
    count_available: z.ZodOptional<z.ZodNumber>;
    count_total: z.ZodOptional<z.ZodNumber>;
}, "strip", z.ZodTypeAny, {
    connector_type_id?: number | undefined;
    status_id?: number | undefined;
    current_type_id?: number | undefined;
    power_kw?: number | undefined;
    voltage?: number | undefined;
    amperage?: number | undefined;
    count_available?: number | undefined;
    count_total?: number | undefined;
}, {
    connector_type_id?: number | undefined;
    status_id?: number | undefined;
    current_type_id?: number | undefined;
    power_kw?: number | undefined;
    voltage?: number | undefined;
    amperage?: number | undefined;
    count_available?: number | undefined;
    count_total?: number | undefined;
}>;
export type UpdateChargerRequest = z.infer<typeof UpdateChargerRequestSchema>;
export declare const LookupEntrySchema: z.ZodObject<{
    id: z.ZodNumber;
    name: z.ZodString;
}, "strip", z.ZodTypeAny, {
    name: string;
    id: number;
}, {
    name: string;
    id: number;
}>;
export type LookupEntry = z.infer<typeof LookupEntrySchema>;
//# sourceMappingURL=index.d.ts.map