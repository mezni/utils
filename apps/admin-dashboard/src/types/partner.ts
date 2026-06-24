import type { BaseEntity } from './common';

export interface Partner extends BaseEntity {
  name: string;
  is_valid: boolean;
  deleted_at: string | null;
}

export interface CreatePartnerRequest {
  name: string;
  status: string;
  is_valid: boolean;
}
