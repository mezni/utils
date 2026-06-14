// source/apps/shared-mobile/src/index.ts

export * from './constants';
export * from './types';

/**
 * Utility tool to validate user locations against the local service boundaries.
 */
export function verifyCoordinateWithinTunisia(lon: number, lat: number): boolean {
  return (
    lon >= 7.0000 && lon <= 12.0000 &&
    lat >= 30.0000 && lat <= 38.0000
  );
}
