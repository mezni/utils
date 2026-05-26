import { useEffect, useRef } from "react"
import L from "leaflet"
import "leaflet/dist/leaflet.css"

export interface StationMarkerData {
  id: string
  name: string
  city: string
  coordinates: [number, number]
  chargerCount: number
}

interface BaseMapProps {
  stations: StationMarkerData[]
  center?: [number, number]
  zoom?: number
  onStationClick?: (id: string) => void
  highlightedStationId?: string | null
  onMapReady?: (map: L.Map) => void
}

const boltIconSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="white" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z"/></svg>`

const defaultIcon = L.divIcon({
  className: "",
  html: `<div style="display:flex;align-items:center;justify-content:center;width:32px;height:32px;border-radius:50%;background:#22c55e;border:3px solid white;box-shadow:0 2px 8px rgba(0,0,0,0.25);">${boltIconSvg}</div>`,
  iconSize: [32, 32],
  iconAnchor: [16, 16],
})

const highlightIcon = L.divIcon({
  className: "",
  html: `<div style="display:flex;align-items:center;justify-content:center;width:40px;height:40px;border-radius:50%;background:#f59e0b;border:3px solid white;box-shadow:0 2px 12px rgba(0,0,0,0.35);">${boltIconSvg}</div>`,
  iconSize: [40, 40],
  iconAnchor: [20, 20],
})

export function BaseMap({
  stations,
  center = [33.8869, 9.5375],
  zoom = 7,
  onStationClick,
  highlightedStationId,
  onMapReady,
}: BaseMapProps) {
  const mapRef = useRef<HTMLDivElement>(null)
  const mapInstanceRef = useRef<L.Map | null>(null)
  const markersRef = useRef<Map<string, L.Marker>>(new Map())

  useEffect(() => {
    if (!mapRef.current || mapInstanceRef.current) return

    const map = L.map(mapRef.current, {
      center: center as L.LatLngExpression,
      zoom,
      zoomControl: true,
    })

    L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OSM</a>, &copy; CARTO',
      subdomains: "abcd",
      maxZoom: 19,
    }).addTo(map)

    mapInstanceRef.current = map
    onMapReady?.(map)

    return () => {
      map.remove()
      mapInstanceRef.current = null
    }
  }, [center, zoom])

  useEffect(() => {
    const map = mapInstanceRef.current
    if (!map) return

    markersRef.current.forEach((m) => m.remove())
    markersRef.current = new Map()

    stations.forEach((station) => {
      const marker = L.marker(station.coordinates, { icon: defaultIcon }).addTo(map)

      marker.bindPopup(`
        <div style="font-family:system-ui,sans-serif;padding:4px;min-width:160px;">
          <div style="font-weight:600;font-size:14px;margin-bottom:4px;">${station.name}</div>
          <div style="font-size:12px;color:#666;margin-bottom:4px;">${station.city}</div>
          <div style="font-size:12px;color:#888;margin-bottom:8px;">${station.chargerCount} charger(s)</div>
          <a href="/stations/${station.id}/chargers" style="font-size:12px;color:#22c55e;font-weight:500;text-decoration:none;">View Chargers →</a>
        </div>
      `)

      marker.on("click", () => onStationClick?.(station.id))

      markersRef.current.set(station.id, marker)
    })

    if (stations.length > 0) {
      const group = L.featureGroup(Array.from(markersRef.current.values()))
      map.fitBounds(group.getBounds().pad(0.2))
      if (map.getZoom() > zoom) {
        map.setZoom(zoom)
      }
    }
  }, [stations, zoom, onStationClick])

  useEffect(() => {
    markersRef.current.forEach((marker, id) => {
      marker.setIcon(id === highlightedStationId ? highlightIcon : defaultIcon)
    })
  }, [highlightedStationId])

  return (
    <div className="h-full w-full">
      <div ref={mapRef} className="h-full w-full rounded-xl" style={{ minHeight: "400px" }} />
    </div>
  )
}
