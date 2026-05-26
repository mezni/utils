import { useState, useEffect, useCallback } from "react"
import { ScrollableTable, ConfirmDeleteModal } from "@bornemap/ui"
import { StationFormModal } from "./station-form-modal"
import type { StationMarkerData } from "../map/base-map"

interface Station {
  id: string
  name: string
  city: string
  latitude: number
  longitude: number
  owner_id: string
  is_operational: boolean
  is_test: boolean
}

interface StationsTableProps {
  refreshKey?: number
  onDelete?: (target: { id: string; label: string }) => void
  onStationSelect?: (id: string) => void
  highlightedStationId?: string | null
  onStationsUpdate?: (stations: StationMarkerData[]) => void
}

export function StationsTable({
  refreshKey = 0,
  onDelete,
  onStationSelect,
  highlightedStationId,
  onStationsUpdate,
}: StationsTableProps) {
  const [stations, setStations] = useState<Station[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [showCreate, setShowCreate] = useState(false)
  const [editingStation, setEditingStation] = useState<Station | null>(null)
  const [deleteTarget, setDeleteTarget] = useState<{ id: string; label: string } | null>(null)

  const fetchStations = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await fetch("/api/v1/stations")
      if (!res.ok) throw new Error("Failed to fetch stations")
      const json = await res.json()
      const data = json.data || json
      setStations(data)
      if (onStationsUpdate) {
        onStationsUpdate(
          data.map((s: Station) => ({
            id: s.id,
            name: s.name,
            city: s.city,
            coordinates: [s.latitude, s.longitude] as [number, number],
            chargerCount: 0,
          }))
        )
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load stations")
    } finally {
      setLoading(false)
    }
  }, [onStationsUpdate])

  useEffect(() => {
    fetchStations()
  }, [fetchStations, refreshKey])

  const handleDelete = async () => {
    if (!deleteTarget) return
    try {
      const res = await fetch(`/api/v1/stations/${deleteTarget.id}`, { method: "DELETE" })
      if (!res.ok && res.status !== 204) throw new Error("Failed to delete station")
      setDeleteTarget(null)
      fetchStations()
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to delete station")
    }
  }

  if (loading) {
    return <div className="p-4 text-sm text-gray-400">Loading stations...</div>
  }

  if (error) {
    return (
      <div className="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-600">
        <p>{error}</p>
        <button onClick={fetchStations} className="mt-2 text-sm font-medium text-red-700 underline">
          Retry
        </button>
      </div>
    )
  }

  const columns = [
    { key: "id", label: "ID" },
    { key: "name", label: "Name" },
    { key: "city", label: "City" },
    { key: "latitude", label: "Lat" },
    { key: "longitude", label: "Lng" },
    { key: "is_operational", label: "Operational" },
    { key: "is_test", label: "Test" },
    { key: "actions", label: "Actions" },
  ]

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <h3 className="text-lg font-semibold text-gray-900">Stations</h3>
        <button
          onClick={() => setShowCreate(true)}
          className="rounded-lg bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent/90"
        >
          Create Station
        </button>
      </div>

      {stations.length === 0 ? (
        <div className="rounded-lg border-2 border-dashed border-gray-200 p-8 text-center text-sm text-gray-400">
          No stations yet. Create your first station to get started.
        </div>
      ) : (
        <ScrollableTable>
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="border-b border-gray-200 text-xs uppercase text-gray-500">
                {columns.map((col) => (
                  <th key={col.key} className="px-4 py-3 font-medium">
                    {col.label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {stations.map((station) => (
                <tr
                  key={station.id}
                  id={`station-row-${station.id}`}
                  onClick={() => onStationSelect?.(station.id)}
                  className={`cursor-pointer border-b border-gray-100 transition-colors hover:bg-gray-50 ${
                    highlightedStationId === station.id ? "bg-amber-50" : ""
                  }`}
                >
                  <td className="px-4 py-3 font-mono text-xs text-gray-500">{station.id}</td>
                  <td className="px-4 py-3 font-medium text-gray-900">{station.name}</td>
                  <td className="px-4 py-3 text-gray-600">{station.city}</td>
                  <td className="px-4 py-3 text-gray-600">{station.latitude.toFixed(4)}</td>
                  <td className="px-4 py-3 text-gray-600">{station.longitude.toFixed(4)}</td>
                  <td className="px-4 py-3">
                    <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${
                      station.is_operational ? "bg-green-100 text-green-700" : "bg-gray-100 text-gray-600"
                    }`}>
                      {station.is_operational ? "Yes" : "No"}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-gray-600">{station.is_test ? "Yes" : "No"}</td>
                  <td className="px-4 py-3">
                    <div className="flex gap-2">
                      <button
                        onClick={() => setEditingStation(station)}
                        className="text-xs font-medium text-accent hover:text-accent/80"
                      >
                        Edit
                      </button>
                      <button
                        onClick={() => {
                          const target = { id: station.id, label: station.name }
                          setDeleteTarget(target)
                          onDelete?.(target)
                        }}
                        className="text-xs font-medium text-red-500 hover:text-red-700"
                      >
                        Delete
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </ScrollableTable>
      )}

      <ConfirmDeleteModal
        isOpen={!!deleteTarget}
        resourceId={deleteTarget?.id ?? ""}
        resourceLabel={deleteTarget?.label ?? ""}
        onConfirm={handleDelete}
        onCancel={() => setDeleteTarget(null)}
      />

      {showCreate && (
        <StationFormModal
          onClose={() => setShowCreate(false)}
          onSaved={() => { setShowCreate(false); fetchStations() }}
        />
      )}

      {editingStation && (
        <StationFormModal
          station={editingStation}
          onClose={() => setEditingStation(null)}
          onSaved={() => { setEditingStation(null); fetchStations() }}
        />
      )}
    </div>
  )
}
