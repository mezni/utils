import { ScrollableTable } from "@bornemap/ui"
import { useEffect, useState } from "react"

interface Station {
  id: string
  name: string
  city: string
  owner_name: string
  latitude: number
  longitude: number
  is_operational: boolean
  is_test: boolean
}

interface StationRow {
  id: string
  name: string
  city: string
  owner_name: string
  latitude: number
  longitude: number
  is_operational: boolean
  is_test: boolean
}

interface StationsTableProps {
  refreshKey: number
  onDelete: (target: { id: string; label: string }) => void
  highlightedStationId: string | null
  onStationSelect: (id: string) => void
  onDataLoaded?: (stations: StationRow[]) => void
}

export function StationsTable({ refreshKey, onDelete, highlightedStationId, onStationSelect, onDataLoaded }: StationsTableProps) {
  const [stations, setStations] = useState<Station[]>([])
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    setIsLoading(true)
    fetch("/api/v1/stations")
      .then((r) => r.json())
      .then((body) => { if (!cancelled) { const d = body.data || []; setStations(d); onDataLoaded?.(d) } })
      .catch(() => { if (!cancelled) { setStations([]); onDataLoaded?.([]) } })
      .finally(() => { if (!cancelled) setIsLoading(false) })
    return () => { cancelled = true }
  }, [refreshKey])

  if (isLoading) return <div className="h-48 animate-pulse rounded-xl bg-gray-100" />
  if (stations.length === 0) return <div className="flex items-center justify-center rounded-xl border border-dashed border-gray-300 p-12"><p className="text-sm text-gray-500">No stations found</p></div>

  return (
    <ScrollableTable>
      <table className="w-full text-left text-sm">
        <thead>
          <tr className="border-b border-gray-200 text-xs font-medium uppercase text-gray-500">
            <th className="px-4 py-3">ID</th>
            <th className="px-4 py-3">Name</th>
            <th className="px-4 py-3">City</th>
            <th className="px-4 py-3">Owner</th>
            <th className="px-4 py-3">Coordinates</th>
            <th className="px-4 py-3">Operational</th>
            <th className="px-4 py-3">Test</th>
            <th className="px-4 py-3" />
          </tr>
        </thead>
        <tbody>
          {stations.map((s) => (
            <tr
              key={s.id}
              onClick={() => onStationSelect(s.id)}
              className={`cursor-pointer border-b border-gray-100 hover:bg-gray-50 ${highlightedStationId === s.id ? "bg-blue-50" : ""}`}
            >
              <td className="px-4 py-3 font-mono text-xs text-gray-600">{s.id}</td>
              <td className="px-4 py-3 font-medium text-gray-900">{s.name}</td>
              <td className="px-4 py-3 text-gray-700">{s.city}</td>
              <td className="px-4 py-3 text-gray-700">{s.owner_name}</td>
              <td className="px-4 py-3 font-mono text-xs text-gray-500">{s.latitude.toFixed(4)}, {s.longitude.toFixed(4)}</td>
              <td className="px-4 py-3">{s.is_operational ? <span className="rounded-full bg-green-100 px-2 py-0.5 text-xs font-medium text-green-700">Yes</span> : <span className="rounded-full bg-gray-100 px-2 py-0.5 text-xs font-medium text-gray-600">No</span>}</td>
              <td className="px-4 py-3">{s.is_test ? <span className="rounded-full bg-amber-100 px-2 py-0.5 text-xs font-medium text-amber-700">Yes</span> : <span className="text-xs text-gray-400">—</span>}</td>
              <td className="px-4 py-3">
                <button onClick={(e) => { e.stopPropagation(); onDelete({ id: s.id, label: s.name }) }} className="text-xs font-medium text-red-600 hover:text-red-800">Delete</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </ScrollableTable>
  )
}
