export type StationStatus = "draft" | "active" | "inactive" | "closed";
export type ChargerType = "ac" | "dc";
export type ConnectorStandard = "ccs2" | "type2" | "chademo";

export interface Station {
  id: string;
  name: string;
  latitude: number;
  longitude: number;
  status: StationStatus;
}
