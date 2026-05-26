import { StationsTable } from "../../components/data/stations-table"
import { StationFormModal } from "../../components/data/station-form-modal"
import { BaseMap } from "../../components/map/base-map"
import { ConfirmDeleteModal } from "@bornemap/ui"
import { useState, useCallback } from "react"
import type { Map as LeafletMap } from "leaflet"

export function StationsPage() {
  const [refreshKey, setRefreshKey] = useState(0)
  const [deleteTarget, setDeleteTarget] = useState<{ id: string; label: string } | null>(null)
  const [highlightedStationId, setHighlightedStationId] = useState<string | null>(null)
  const [showCreate, setShowCreate] = useState(false)
  const [stations, setStations] = useState<StationData[]>([])
  const [mapInstance, setMapInstance] = useState<LeafletMap | null>(null)

  const handleStationSelect = useCallback((id: string) => {
    setHighlightedStationId(id)
    const station = stations.find((s) => s.id === id)
    if (station && mapInstance) {
      mapInstance.flyTo([station.latitude, station.longitude], 12, { duration: 1 })
    }
  }, [stations, mapInstance])

  const handleMarkerClick = useCallback((id: string) => {
    setHighlightedStationId(id)
  }, [])

  const handleDelete = async () => {
    if (!deleteTarget) return
    await fetch(`/api/v1/stations/${deleteTarget.id}`, { method: "DELETE" })
    setDeleteTarget(null)
    setRefreshKey((k) => k + 1)
  }

  return (
    <div>
      <div className="mb-4 flex justify-end">
        <button
          onClick={() => setShowCreate(true)}
          className="rounded-lg bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent/90"
        >
          Create Station
        </button>
      </div>
      <div className="mb-6 overflow-hidden rounded-xl border border-gray-200">
        <div className="h-[300px]">
          <BaseMap
            stations={stations.map((s) => ({
              id: s.id,
              name: s.name,
              city: s.city,
              coordinates: [s.latitude, s.longitude],
              chargerCount: 0,
            }))}
            onStationClick={handleMarkerClick}
            onMapReady={setMapInstance}
          />
        </div>
      </div>
      <StationsTable
        refreshKey={refreshKey}
        onDelete={setDeleteTarget}
        highlightedStationId={highlightedStationId}
        onStationSelect={handleStationSelect}
        onDataLoaded={setStations}
      />
      {showCreate && (
        <StationFormModal
          onClose={() => setShowCreate(false)}
          onSaved={() => { setShowCreate(false); setRefreshKey((k) => k + 1) }}
        />
      )}
      <ConfirmDeleteModal
        isOpen={!!deleteTarget}
        resourceId={deleteTarget?.id ?? ""}
        resourceLabel={deleteTarget?.label ?? ""}
        onConfirm={handleDelete}
        onCancel={() => setDeleteTarget(null)}
      />
    </div>
  )
}

interface StationData {
  id: string
  name: string
  city: string
  latitude: number
  longitude: number
}

