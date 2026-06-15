export * from './constants';
export * from './types';

export function verifyCoordinateWithinTunisia(lon: number, lat: number): boolean {
  return (
    lon >= 7.0000 && lon <= 12.0000 &&
    lat >= 30.0000 && lat <= 38.0000
  );
}
