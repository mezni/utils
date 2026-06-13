import { useMapStore } from '../store/useMapStore'

export interface Station {
  id: string
  name: string
  address: string
  geometry: {
    type: 'Point'
    coordinates: [number, number]
  }
  distance_km: number
}

export async function fetchStationsByRadius(
  lat: number,
  lng: number,
  radius: number = 10,
): Promise<Station[]> {
  // This is a client-side implementation that will be replaced with real API calls
  // For now, return mock data

  // Simulate network delay
  await new Promise<void>(resolve => setTimeout(() => resolve(), 300))

  // Return mock stations within radius
  const stations: Station[] = []
  const numStations = Math.floor(Math.random() * 10) + 5

  for (let i = 0; i < numStations; i++) {
    const distance = Math.random() * radius
    stations.push({
      id: `STA-${Math.random().toString(36).substring(2, 11)}`,
      name: `Station ${Math.floor(Math.random() * 100)}`,
      address: `123 Main Street, Tunis, Tunisia`,
      geometry: {
        type: 'Point',
        coordinates: [
          lng + (Math.random() - 0.5) * (distance / 111),
          lat + (Math.random() - 0.5) * (distance / 111),
        ],
      },
      distance_km: parseFloat(distance.toFixed(2)),
    })
  }

  return stations
}

export function updateMarkerDistances(
  lat: number,
  lng: number,
  markers: any[],
) {
  return markers.map((marker) => {
    const distance = calculateDistance(
      lat,
      lng,
      marker.geometry?.coordinates[1] || lat,
      marker.geometry?.coordinates[0] || lng,
    )
    return {
      ...marker,
      distance_km: parseFloat(distance.toFixed(2)),
    }
  })
}

// Helper function to calculate distance between two coordinates
function calculateDistance(
  lat1: number,
  lng1: number,
  lat2: number,
  lng2: number,
): number {
  const R = 6371 // Earth's radius in kilometers
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


