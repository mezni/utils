import type { Station } from './stationListService'

export function formatStationMarker(station: Station): {
  position: [number, number]
  title: string
  description: string
} {
  return {
    position: [
      station.geometry.coordinates[1],
      station.geometry.coordinates[0],
    ],
    title: station.name,
    description: station.address,
  }
}

export function calculateBounds(
  stations: Station[],
): [[number, number], [number, number]] {
  if (stations.length === 0) {
    return [[36.5, 9.5], [37.1, 10.8]]
  }

  const lats = stations.map((s) => s.geometry.coordinates[1])
  const lngs = stations.map((s) => s.geometry.coordinates[0])

  return [
    [Math.min(...lats), Math.min(...lngs)],
    [Math.max(...lats), Math.max(...lngs)],
  ]
}