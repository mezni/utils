import type { BaseEntity } from './common';

export interface Station extends BaseEntity {
  partner_id: string;
  name: string;
  location: string | null;
  deleted_at: string | null;
}

export interface CreateStationRequest {
  partner_id: string;
  name: string;
  location?: string;
  status: string;
}
