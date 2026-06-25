import type { AdminChargerDto, CreateChargerRequest, UpdateChargerRequest } from "@bornemap/domain-types";
export interface PaginationInfo {
    page: number;
    per_page: number;
    total: number;
    total_pages: number;
}
export interface PaginatedResponse<T> {
    data: T[];
    pagination: PaginationInfo;
}
interface ListParams {
    page?: number;
    per_page?: number;
    station_id?: string;
}
export declare function listChargers(params?: ListParams): Promise<PaginatedResponse<AdminChargerDto>>;
export declare function getCharger(id: string): Promise<AdminChargerDto>;
export declare function createCharger(data: CreateChargerRequest): Promise<AdminChargerDto>;
export declare function updateCharger(id: string, data: UpdateChargerRequest): Promise<AdminChargerDto>;
export declare function deleteCharger(id: string): Promise<void>;
export {};
//# sourceMappingURL=chargers.d.ts.map