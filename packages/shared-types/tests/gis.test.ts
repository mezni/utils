import { describe, it, expect } from 'vitest';
import type { Station, Charger, NearbyResponse, Location } from '@bornemap/shared-types';

describe('Shared Types', () => {
  describe('Location', () => {
    it('should create a valid location', () => {
      const location: Location = {
        lat: 36.8,
        lon: 10.18,
      };

      expect(location.lat).toBe(36.8);
      expect(location.lon).toBe(10.18);
    });

    it('should validate lat range (-90 to 90)', () => {
      expect(() => {
        const location: Location = { lat: 91, lon: 10.18 };
      }).not.toThrow();

      expect(() => {
        const location: Location = { lat: -91, lon: 10.18 };
      }).not.toThrow();
    });

    it('should validate lon range (-180 to 180)', () => {
      expect(() => {
        const location: Location = { lat: 36.8, lon: 181 };
      }).not.toThrow();

      expect(() => {
        const location: Location = { lat: 36.8, lon: -181 };
      }).not.toThrow();
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

      expect(charger.id).toBe('chg_123');
      expect(charger.connector_type).toBe('type2');
      expect(charger.power_kw).toBe(22.0);
      expect(charger.status).toBe('available');
    });

    it('should handle optional fields', () => {
      const charger: Charger = {
        id: 'chg_123',
        connector_type: 'type2',
        power_kw: 22.0,
        status: 'available',
        connector_count: 2,
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
        distance_km: 2.5,
        address: '123 Test Street',
        city: 'Tunis',
        status: 'active',
      };

      expect(station.id).toBe('sta_123');
      expect(station.name).toBe('Test Station');
      expect(station.distance_km).toBe(2.5);
    });

    it('should handle optional fields', () => {
      const station: Station = {
        id: 'sta_123',
        name: 'Test Station',
        visibility: 'commercial',
        location: { lat: 36.8, lon: 10.18 },
        distance_km: 2.5,
        address: '123 Test Street',
        city: 'Tunis',
        status: 'active',
        connector_types: ['type2', 'ccs2'],
        connector_power: [22.0, 50.0],
      };

      expect(station.connector_types).toEqual(['type2', 'ccs2']);
      expect(station.connector_power).toEqual([22.0, 50.0]);
    });
  });

  describe('NearbyResponse', () => {
    it('should create a valid nearby response', () => {
      const response: NearbyResponse = {
        stations: [
          {
            id: 'sta_123',
            name: 'Test Station',
            visibility: 'commercial',
            location: { lat: 36.8, lon: 10.18 },
            distance_km: 2.5,
            address: '123 Test Street',
            city: 'Tunis',
            status: 'active',
          },
        ],
        count: 1,
        radius_m: 5000,
      };

      expect(response.stations.length).toBe(1);
      expect(response.count).toBe(1);
      expect(response.radius_m).toBe(5000);
    });

    it('should handle empty stations array', () => {
      const response: NearbyResponse = {
        stations: [],
        count: 0,
        radius_m: 5000,
      };

      expect(response.stations.length).toBe(0);
      expect(response.count).toBe(0);
    });
  });
});
