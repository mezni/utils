import { describe, it, expect } from 'vitest';
import type { Station, Charger, NearbyResponse, Location, BoundingBox, ImportResponse, ChargerStatus, Visibility } from '@bornemap/shared-types';

describe('Shared Types', () => {
  describe('Location', () => {
    it('should create a valid location', () => {
      const location: Location = { lat: 36.8, lon: 10.18 };
      expect(location.lat).toBe(36.8);
      expect(location.lon).toBe(10.18);
    });
  });

  describe('Charger', () => {
    it('should create a valid charger', () => {
      const charger: Charger = {
        id: 'chg_123',
        connector_type: 'type2',
        power_kw: 22.0,
        status: 'available',
      };
      expect(charger.connector_type).toBe('type2');
      expect(charger.power_kw).toBe(22.0);
    });

    it('should handle optional connector_count field', () => {
      const charger: Charger = {
        id: 'chg_123',
        connector_type: 'type2',
        connector_count: 2,
        power_kw: 22.0,
        status: 'available',
      };
      expect(charger.connector_count).toBe(2);
    });
  });

  describe('Station', () => {
    it('should create a valid station', () => {
      const station: Station = {
        id: 'sta_123',
        name: 'Test Station',
        visibility: 'commercial',
        location: { lat: 36.8, lon: 10.18 },
        distance_m: 2500,
        address: '123 Test Street',
        city: 'Tunis',
        status: 'active',
      };
      expect(station.id).toBe('sta_123');
      expect(station.distance_m).toBe(2500);
    });

    it('should accept private_home visibility', () => {
      const station: Station = {
        id: 'sta_124',
        name: 'Home Charger',
        visibility: 'private_home',
        location: { lat: 36.8, lon: 10.18 },
        distance_m: 500,
        city: 'Tunis',
        status: 'active',
      };
      expect(station.visibility).toBe('private_home');
    });

    it('should accept optional chargers array', () => {
      const station: Station = {
        id: 'sta_123',
        name: 'Test Station',
        visibility: 'commercial',
        location: { lat: 36.8, lon: 10.18 },
        distance_m: 2500,
        city: 'Tunis',
        status: 'active',
        chargers: [
          { id: 'chg_1', connector_type: 'type2', power_kw: 22, status: 'available' },
          { id: 'chg_2', connector_type: 'ccs2', power_kw: 50, status: 'available' },
        ],
      };
      expect(station.chargers).toHaveLength(2);
    });
  });

  describe('NearbyResponse', () => {
    it('should create a valid nearby response', () => {
      const response: NearbyResponse = {
        stations: [{ id: 'sta_123', name: 'Test', visibility: 'commercial', location: { lat: 36.8, lon: 10.18 }, distance_m: 2500, city: 'Tunis', status: 'active' }],
        count: 1,
        radius_m: 5000,
      };
      expect(response.stations).toHaveLength(1);
      expect(response.count).toBe(1);
    });

    it('should handle empty stations array', () => {
      const response: NearbyResponse = { stations: [], count: 0, radius_m: 5000 };
      expect(response.stations).toHaveLength(0);
      expect(response.count).toBe(0);
    });
  });

  describe('BoundingBox', () => {
    it('should create a valid bounding box', () => {
      const bbox: BoundingBox = { min_lat: 30, min_lon: 7, max_lat: 37, max_lon: 11 };
      expect(bbox.min_lat).toBe(30);
      expect(bbox.max_lat).toBe(37);
    });
  });

  describe('ImportResponse', () => {
    it('should create a valid import response', () => {
      const resp: ImportResponse = {
        data: { import_id: 'imp_123', region: 'tunisia', stations_imported: 10, stations_updated: 2, stations_failed: 0, status: 'completed' },
        meta: { request_id: 'req_1', timestamp: new Date().toISOString() },
      };
      expect(resp.data.status).toBe('completed');
      expect(resp.data.stations_imported).toBe(10);
    });
  });
});
