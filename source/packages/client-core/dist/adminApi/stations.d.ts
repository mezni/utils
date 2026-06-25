import type { AdminStationDto, CreateStationRequest, UpdateStationRequest } from "@bornemap/domain-types";
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
    partner_id?: string;
}
export declare function listStations(params?: ListParams): Promise<PaginatedResponse<AdminStationDto>>;
export declare function getStation(id: string): Promise<AdminStationDto>;
export declare function createStation(data: CreateStationRequest): Promise<AdminStationDto>;
export declare function updateStation(id: string, data: UpdateStationRequest): Promise<AdminStationDto>;
export declare function deleteStation(id: string): Promise<void>;
export {};
//# sourceMappingURL=stations.d.ts.map