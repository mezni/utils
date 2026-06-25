import type { AdminPartnerDto, CreatePartnerRequest, UpdatePartnerRequest } from "@bornemap/domain-types";
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
    search?: string;
}
export declare function listPartners(params?: ListParams): Promise<PaginatedResponse<AdminPartnerDto>>;
export declare function getPartner(id: string): Promise<AdminPartnerDto>;
export declare function createPartner(data: CreatePartnerRequest): Promise<AdminPartnerDto>;
export declare function updatePartner(id: string, data: UpdatePartnerRequest): Promise<AdminPartnerDto>;
export declare function deletePartner(id: string): Promise<void>;
export {};
//# sourceMappingURL=partners.d.ts.map