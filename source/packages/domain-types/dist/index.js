import { z } from "zod";
/* ─── Consumer-facing (existing) ─── */
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
/* ─── Admin DTOs ─── */
export const AdminPartnerSchema = z.object({
    partner_id: z.string(),
    name: z.string(),
    partner_type: z.string().nullable(),
    support_phone: z.string().nullable(),
    support_email: z.string().nullable(),
    is_verified: z.boolean(),
    created_at: z.string(),
    updated_at: z.string().nullable(),
});
export const AdminStationSchema = z.object({
    station_id: z.string(),
    osm_id: z.number().nullable(),
    partner_id: z.string().nullable(),
    name: z.string(),
    address: z.string().nullable(),
    lat: z.number(),
    lon: z.number(),
    created_at: z.string(),
    updated_at: z.string().nullable(),
});
export const AdminChargerSchema = z.object({
    charger_id: z.string(),
    station_id: z.string(),
    connector_type_id: z.number(),
    status_id: z.number(),
    current_type_id: z.number(),
    power_kw: z.number().nullable(),
    voltage: z.number().nullable(),
    amperage: z.number().nullable(),
    count_available: z.number(),
    count_total: z.number(),
    created_at: z.string(),
    updated_at: z.string().nullable(),
});
export const PaginationSchema = z.object({
    page: z.number(),
    per_page: z.number(),
    total: z.number(),
    total_pages: z.number(),
});
export const PaginatedResponseSchema = (item) => z.object({
    data: z.array(item),
    pagination: PaginationSchema,
});
/* ─── Admin Create / Update requests ─── */
export const CreatePartnerRequestSchema = z.object({
    name: z.string().min(1),
    partner_type: z.string().optional(),
    support_phone: z.string().optional(),
    support_email: z.string().optional(),
});
export const UpdatePartnerRequestSchema = z.object({
    name: z.string().optional(),
    partner_type: z.string().optional(),
    support_phone: z.string().optional(),
    support_email: z.string().optional(),
});
export const CreateStationRequestSchema = z.object({
    name: z.string().min(1),
    lat: z.number(),
    lon: z.number(),
    osm_id: z.number().optional(),
    partner_id: z.string().optional(),
    address: z.string().optional(),
});
export const UpdateStationRequestSchema = z.object({
    name: z.string().optional(),
    address: z.string().optional(),
    lat: z.number().optional(),
    lon: z.number().optional(),
    partner_id: z.string().optional(),
});
export const CreateChargerRequestSchema = z.object({
    station_id: z.string().min(1),
    connector_type_id: z.number().int(),
    status_id: z.number().int(),
    current_type_id: z.number().int(),
    power_kw: z.number().optional(),
    voltage: z.number().optional(),
    amperage: z.number().optional(),
    count_available: z.number().int().optional(),
    count_total: z.number().int().optional(),
});
export const UpdateChargerRequestSchema = z.object({
    connector_type_id: z.number().int().optional(),
    status_id: z.number().int().optional(),
    current_type_id: z.number().int().optional(),
    power_kw: z.number().optional(),
    voltage: z.number().optional(),
    amperage: z.number().optional(),
    count_available: z.number().int().optional(),
    count_total: z.number().int().optional(),
});
/* ─── Lookup types ─── */
export const LookupEntrySchema = z.object({
    id: z.number(),
    name: z.string(),
});
//# sourceMappingURL=index.js.map