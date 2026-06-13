import { useEffect, useRef } from 'react'

let mapInstance: any = null
let markersLayer: any = null

export function initMap() {
  return new Promise<void>((resolve, reject) => {
    try {
      // Check if Leaflet is loaded
      if (typeof L === 'undefined') {
        reject(new Error('Leaflet is not loaded'))
        return
      }

      // Get the map container
      const mapContainer = document.getElementById('map')
      if (!mapContainer) {
        reject(new Error('Map container not found'))
        return
      }

      // Initialize the map
      mapInstance = L.map('map').setView([36.8065, 10.1815], 14)

      // Add OpenStreetMap tile layer
      L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
        maxZoom: 19,
      }).addTo(mapInstance)

      console.log('Map initialized successfully')
      resolve()
    } catch (error) {
      console.error('Failed to initialize map:', error)
      reject(error)
    }
  })
}

export function loadMapStyle(isDarkMode: boolean) {
  if (!mapInstance) return

  // Update map style based on theme
  if (isDarkMode) {
    // Dark mode style using CartoDB Dark Matter tiles
    mapInstance.eachLayer((layer: any) => {
      if (layer instanceof L.TileLayer) {
        layer.setUrl('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png')
      }
    })
  } else {
    // Light mode style using OpenStreetMap
    mapInstance.eachLayer((layer: any) => {
      if (layer instanceof L.TileLayer) {
        layer.setUrl('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png')
      }
    })
  }

  mapInstance.invalidateSize()
}

export function addMarkerToMap(
  lat: number,
  lng: number,
  title: string,
  description?: string,
) {
  if (!mapInstance) {
    console.error('Map not initialized')
    return null
  }

  const marker = L.marker([lat, lng])
    .addTo(mapInstance)
    .bindPopup(`<b>${title}</b>`)
    .bindTooltip(description || title, {
      permanent: false,
      direction: 'top',
    })

  return marker
}

export function clearMarkers() {
  if (!mapInstance) return

  mapInstance.eachLayer((layer: any) => {
    if (layer instanceof L.Marker) {
      mapInstance.removeLayer(layer)
    }
  })
}

export function updateMapCenter(lat: number, lng: number, zoom: number = 14) {
  if (!mapInstance) return

  mapInstance.setView([lat, lng], zoom)
}

export function getMapCenter() {
  if (!mapInstance) return null

  const center = mapInstance.getCenter()
  return {
    lat: center.lat,
    lng: center.lng,
  }
}

export function addMarkersToMap(markers: Array<{
  lat: number
  lng: number
  title: string
  description?: string
}>) {
  clearMarkers()

  markers.forEach((marker) => {
    addMarkerToMap(
      marker.lat,
      marker.lng,
      marker.title,
      marker.description,
    )
  })
}

export function subscribeToMapEvents(callback: () => void) {
  if (!mapInstance) return

  mapInstance.on('moveend', callback)
  mapInstance.on('zoomend', callback)

  return () => {
    if (mapInstance) {
      mapInstance.off('moveend', callback)
      mapInstance.off('zoomend', callback)
    }
  }
}

export function useLeafletMap() {
  const mapRef = useRef<any>(mapInstance)
  const isDarkMode = useRef(false)

  const addMarker = (
    lat: number,
    lng: number,
    title: string,
    description?: string,
  ) => {
    if (mapInstance) {
      return addMarkerToMap(lat, lng, title, description)
    }
    return null
  }

  const clearMarkers = () => {
    if (mapInstance) {
      clearMarkers()
    }
  }

  const updateCenter = (lat: number, lng: number, zoom?: number) => {
    if (mapInstance) {
      updateMapCenter(lat, lng, zoom)
    }
  }

  return {
    addMarker,
    clearMarkers,
    updateCenter,
  }
}
