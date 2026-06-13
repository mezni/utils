import { create } from 'zustand'

export interface UserLocation {
  lat: number
  lng: number
}

export interface Marker {
  id: string
  name: string
  address: string
  distance_km: number
  lat: number
  lng: number
}

interface MapState {
  userLocation: UserLocation | null
  center: UserLocation
  zoom: number
  markers: Marker[]
  searchRadius: number
  setSelectedStationId: (id: string) => void
  setSelectedStation: (marker: Marker | null) => void
  setUserLocation: (location: UserLocation | null) => void
  setCenter: (location: UserLocation) => void
  setZoom: (zoom: number) => void
  addMarkers: (stations: any[]) => void
  updateMarkers: (markers: Marker[]) => void
  updateMarkerDistance: (lat: number, lng: number) => void
  setRadius: (radius: number) => void
  clearMarkers: () => void
}

const INITIAL_CENTER: UserLocation = {
  lat: 36.8065,
  lng: 10.1815,
}

export const useMapStore = create<MapState>((set) => ({
  userLocation: null,
  center: INITIAL_CENTER,
  zoom: 14,
  markers: [],
  searchRadius: 10,
  setSelectedStationId: (id) => {
    // Find and set the selected station by ID
    const selectedStation = useMapStore.getState().markers.find((m) => m.id === id)
    set({ markers: selectedStation ? [selectedStation] : [] })
  },
  setSelectedStation: (marker) => {
    set({ markers: marker ? [marker] : [] })
  },
  setUserLocation: (location) => {
    set({ userLocation: location })
    if (location && location.lat !== INITIAL_CENTER.lat) {
      set({
        center: location,
        zoom: 14,
      })
    }
  },
  setCenter: (location) => {
    set({ center: location })
  },
  setZoom: (zoom) => {
    set({ zoom })
  },
  addMarkers: (stations) => {
    const newMarkers: Marker[] = stations.map((station: any) => ({
      id: station.id,
      name: station.name,
      address: station.address,
      distance_km: station.distance_km || 0,
      lat: station.geometry?.coordinates[1] || 36.8065,
      lng: station.geometry?.coordinates[0] || 10.1815,
    }))

    set((state) => ({
      markers: [...state.markers, ...newMarkers],
    }))
  },
  updateMarkers: (markers) => {
    set({ markers })
  },
  updateMarkerDistance: (lat: number, lng: number) => {
    set((state) => ({
      markers: state.markers.map((marker) => {
        const distance = calculateDistance(
          lat,
          lng,
          marker.lat,
          marker.lng,
        )
        return {
          ...marker,
          distance_km: distance,
        }
      }),
    }))
  },
  setRadius: (radius) => {
    set({ searchRadius: radius })
  },
  clearMarkers: () => {
    set({ markers: [] })
  },
}))

// Helper function to calculate distance between two coordinates using Haversine formula
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
