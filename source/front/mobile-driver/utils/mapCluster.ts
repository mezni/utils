interface MarkerData {
  key: string
  coordinate: { latitude: number; longitude: number }
  title?: string
  description?: string
  [key: string]: any
}

interface Cluster {
  key: string
  coordinate: { latitude: number; longitude: number }
  stationCount: number
  markers: MarkerData[]
}

export function clusterMarkers(markers: any[], radius: number = 50): Cluster[] {
  if (!markers || markers.length === 0) {
    return []
  }

  const clusters: Cluster[] = []

  markers.forEach((marker, index) => {
    const markerCoord = marker.coordinate || {
      latitude: marker.lat || 0,
      longitude: marker.lng || 0,
    }
    const m: MarkerData = {
      key: marker.key || marker.id || `marker-${index}`,
      coordinate: markerCoord,
      title: marker.title || marker.name,
      description: marker.description || marker.address,
    }

    const alreadyInCluster = clusters.some((cluster) =>
      isMarkerInCluster(m, cluster, radius),
    )

    if (alreadyInCluster) {
      clusters.forEach((cluster) => {
        if (isMarkerInCluster(m, cluster, radius)) {
          cluster.stationCount++
          cluster.markers.push(m)
        }
      })
    } else {
      clusters.push({
        key: `cluster-${index}`,
        coordinate: m.coordinate,
        stationCount: 1,
        markers: [m],
      })
    }
  })

  return clusters
}

function isMarkerInCluster(marker: MarkerData, cluster: Cluster, radius: number): boolean {
  const distance = calculateDistance(
    marker.coordinate.latitude,
    marker.coordinate.longitude,
    cluster.coordinate.latitude,
    cluster.coordinate.longitude,
  )
  return distance <= radius
}

function calculateDistance(
  lat1: number,
  lng1: number,
  lat2: number,
  lng2: number,
): number {
  const R = 6371
  const dLat = (lat2 - lat1) * (Math.PI / 180)
  const dLng = (lng2 - lng1) * (Math.PI / 180)
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1 * (Math.PI / 180)) *
      Math.cos(lat2 * (Math.PI / 180)) *
      Math.sin(dLng / 2) * Math.sin(dLng / 2)
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))
  return R * c
}

export function createClusterMarker(cluster: Cluster) {
  return {
    key: cluster.key,
    coordinate: cluster.coordinate,
    title: `${cluster.stationCount} Stations`,
    description: 'Cluster',
    onPress: () => {
      console.log('Cluster pressed:', cluster.stationCount, 'stations')
    },
  }
}

export function calculateClusterCountText(count: number): string {
  if (count < 5) {
    return `${count}`
  } else if (count < 15) {
    return '10+'
  } else if (count < 50) {
    return '20+'
  } else {
    return '50+'
  }
}

export interface ClusteredMarker {
  marker: MarkerData
  clusterCount: number
}