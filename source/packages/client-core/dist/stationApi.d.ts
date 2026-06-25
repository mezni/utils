import type { StationDto, NearbyParams } from "@bornemap/domain-types";
export interface FetchNearbyStationsParams extends NearbyParams {
    baseUrl: string;
}
export declare function fetchNearbyStations(params: FetchNearbyStationsParams): Promise<StationDto[]>;
//# sourceMappingURL=stationApi.d.ts.map