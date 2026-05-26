import { useState, useEffect, useCallback } from "react"
import { ScrollableTable, ConfirmDeleteModal, SelectSetting } from "@bornemap/ui"
import { ChargerFormModal } from "./charger-form-modal"

interface Charger {
  id: string
  station_id: string
  connector_type_id: string
  power_kw: number
  current_type: string
  status: string
}

interface ChargersTableProps {
  refreshKey?: number
  stationId?: string
  onDelete?: (target: { id: string; label: string }) => void
}

const statusColors: Record<string, string> = {
  available: "bg-green-100 text-green-700",
  occupied: "bg-amber-100 text-amber-700",
  faulted: "bg-red-100 text-red-700",
  offline: "bg-gray-100 text-gray-600",
}

export function ChargersTable({ refreshKey = 0, stationId, onDelete }: ChargersTableProps) {
  const [chargers, setChargers] = useState<Charger[]>([])
  const [stations, setStations] = useState<{ id: string; name: string }[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [showCreate, setShowCreate] = useState(false)
  const [editingCharger, setEditingCharger] = useState<Charger | null>(null)
  const [deleteTarget, setDeleteTarget] = useState<{ id: string; label: string } | null>(null)
  const [stationFilter, setStationFilter] = useState<string>("")

  const fetchChargers = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const url = stationId
        ? `/api/v1/stations/${stationId}/chargers`
        : "/api/v1/chargers"
      const res = await fetch(url)
      if (!res.ok) throw new Error("Failed to fetch chargers")
      const json = await res.json()
      const data = json.data || json
      setChargers(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load chargers")
    } finally {
      setLoading(false)
    }
  }, [stationId])

  const fetchStations = useCallback(async () => {
    try {
      const res = await fetch("/api/v1/stations")
      if (!res.ok) return
      const json = await res.json()
      const data = json.data || json
      setStations(data.map((s: { id: string; name: string }) => ({ id: s.id, name: s.name })))
    } catch {
      // stations list is non-critical
    }
  }, [])

  useEffect(() => {
    fetchChargers()
    if (!stationId) fetchStations()
  }, [fetchChargers, fetchStations, stationId, refreshKey])

  const handleDelete = async () => {
    if (!deleteTarget) return
    try {
      const url = `/api/v1/chargers/${deleteTarget.id}`
      const res = await fetch(url, { method: "DELETE" })
      if (!res.ok && res.status !== 204) throw new Error("Failed to delete charger")
      setDeleteTarget(null)
      fetchChargers()
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to delete charger")
    }
  }

  const filteredChargers = stationFilter
    ? chargers.filter((c) => c.station_id === stationFilter)
    : chargers

  if (loading) {
    return <div className="p-4 text-sm text-gray-400">Loading chargers...</div>
  }

  if (error) {
    return (
      <div className="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-600">
        <p>{error}</p>
        <button onClick={fetchChargers} className="mt-2 text-sm font-medium text-red-700 underline">
          Retry
        </button>
      </div>
    )
  }

  const columns = [
    { key: "id", label: "ID" },
    { key: "station_id", label: "Station" },
    { key: "connector_type_id", label: "Connector" },
    { key: "power_kw", label: "Power (kW)" },
    { key: "current_type", label: "Current" },
    { key: "status", label: "Status" },
    { key: "actions", label: "Actions" },
  ]

  return (
    <div>
      <div className="mb-4 flex items-center justify-between">
        <h3 className="text-lg font-semibold text-gray-900">
          {stationId ? "Station Chargers" : "Chargers"}
        </h3>
        <button
          onClick={() => setShowCreate(true)}
          className="rounded-lg bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent/90"
        >
          Add Charger
        </button>
      </div>

      {!stationId && stations.length > 0 && (
        <div className="mb-4 max-w-xs">
          <SelectSetting
            label="Filter by station"
            value={stationFilter}
            onChange={setStationFilter}
            options={[
              { value: "", label: "All stations" },
              ...stations.map((s) => ({ value: s.id, label: s.name })),
            ]}
          />
        </div>
      )}

      {filteredChargers.length === 0 ? (
        <div className="rounded-lg border-2 border-dashed border-gray-200 p-8 text-center text-sm text-gray-400">
          No chargers found.
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
              {filteredChargers.map((charger) => (
                <tr key={charger.id} className="border-b border-gray-100 transition-colors hover:bg-gray-50">
                  <td className="px-4 py-3 font-mono text-xs text-gray-500">{charger.id}</td>
                  <td className="px-4 py-3 text-gray-600">{charger.station_id}</td>
                  <td className="px-4 py-3 text-gray-600">{charger.connector_type_id}</td>
                  <td className="px-4 py-3 text-gray-900">{charger.power_kw}</td>
                  <td className="px-4 py-3 text-gray-600">{charger.current_type}</td>
                  <td className="px-4 py-3">
                    <span className={`inline-flex rounded-full px-2 py-0.5 text-xs font-medium ${statusColors[charger.status] || "bg-gray-100 text-gray-600"}`}>
                      {charger.status}
                    </span>
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex gap-2">
                      <button
                        onClick={() => setEditingCharger(charger)}
                        className="text-xs font-medium text-accent hover:text-accent/80"
                      >
                        Edit
                      </button>
                      <button
                        onClick={() => {
                          const target = { id: charger.id, label: charger.station_id }
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
        <ChargerFormModal
          stationId={stationId}
          onClose={() => setShowCreate(false)}
          onSaved={() => { setShowCreate(false); fetchChargers() }}
        />
      )}

      {editingCharger && (
        <ChargerFormModal
          charger={editingCharger}
          onClose={() => setEditingCharger(null)}
          onSaved={() => { setEditingCharger(null); fetchChargers() }}
        />
      )}
    </div>
  )
}
