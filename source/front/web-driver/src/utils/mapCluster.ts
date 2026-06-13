export interface ClusterMarker {
  key: string
  coordinate: { latitude: number; longitude: number }
  stationCount: number
  markers: any[]
}

export function clusterMarkers(
  markers: any[],
  radiusMeters: number = 50,
): ClusterMarker[] {
  if (!markers || markers.length === 0) return []

  const clusters: ClusterMarker[] = []
  const processed = new Set<string>()

  for (const marker of markers) {
    if (processed.has(marker.key)) continue

    const nearby: any[] = [marker]
    processed.add(marker.key)

    for (const other of markers) {
      if (processed.has(other.key)) continue
      const distance = getDistance(
        marker.coordinate.latitude,
        marker.coordinate.longitude,
        other.coordinate.latitude,
        other.coordinate.longitude,
      )
      if (distance <= radiusMeters) {
        nearby.push(other)
        processed.add(other.key)
      }
    }

    const avgLat = nearby.reduce((sum, m) => sum + m.coordinate.latitude, 0) / nearby.length
    const avgLng = nearby.reduce((sum, m) => sum + m.coordinate.longitude, 0) / nearby.length

    clusters.push({
      key: `cluster-${clusters.length}`,
      coordinate: { latitude: avgLat, longitude: avgLng },
      stationCount: nearby.length,
      markers: nearby,
    })
  }

  return clusters
}

function getDistance(lat1: number, lng1: number, lat2: number, lng2: number): number {
  const R = 6371000
  const dLat = (lat2 - lat1) * Math.PI / 180
  const dLng = (lng2 - lng1) * Math.PI / 180
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLng / 2) * Math.sin(dLng / 2)
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  return R * c
}