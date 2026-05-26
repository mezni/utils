import { useState, useCallback, useRef } from "react"
import { StationsTable } from "../components/stations/stations-table"
import { BaseMap, type StationMarkerData } from "../components/map/base-map"
import type L from "leaflet"

export function Stations() {
  const [highlightedStationId, setHighlightedStationId] = useState<string | null>(null)
  const [stationMarkers, setStationMarkers] = useState<StationMarkerData[]>([])
  const mapRef = useRef<L.Map | null>(null)

  const handleStationSelect = useCallback((id: string) => {
    setHighlightedStationId(id)
    const station = stationMarkers.find((s) => s.id === id)
    if (station && mapRef.current) {
      mapRef.current.flyTo(station.coordinates, mapRef.current.getZoom() >= 12 ? mapRef.current.getZoom() : 12)
    }
  }, [stationMarkers])

  const handleMarkerClick = useCallback((id: string) => {
    setHighlightedStationId(id)
    document.getElementById(`station-row-${id}`)?.scrollIntoView({ behavior: "smooth", block: "center" })
  }, [])

  return (
    <div className="flex h-full gap-6">
      <div className="flex-1">
        <StationsTable
          onStationSelect={handleStationSelect}
          highlightedStationId={highlightedStationId}
          onStationsUpdate={setStationMarkers}
        />
      </div>
      <div className="w-1/2 min-w-[400px]">
        <div className="sticky top-6 h-[calc(100vh-8rem)] rounded-xl border border-gray-200 overflow-hidden">
          <BaseMap
            stations={stationMarkers}
            onStationClick={handleMarkerClick}
            highlightedStationId={highlightedStationId}
            onMapReady={(map) => { mapRef.current = map }}
          />
        </div>
      </div>
    </div>
  )
}
