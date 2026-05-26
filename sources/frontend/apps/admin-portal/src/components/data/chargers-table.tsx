import { ScrollableTable } from "@bornemap/ui"
import { useEffect, useState } from "react"

interface Charger {
  id: string
  station_name: string
  connector_type_name: string
  power_kw: number
  current_type: string
  status: string
}

interface ChargersTableProps {
  refreshKey: number
  onDelete: (target: { id: string; label: string }) => void
  stationId?: string
}

const statusColors: Record<string, string> = {
  available: "bg-green-100 text-green-700",
  occupied: "bg-amber-100 text-amber-700",
  faulted: "bg-red-100 text-red-700",
  offline: "bg-gray-100 text-gray-600",
}

export function ChargersTable({ refreshKey, onDelete, stationId }: ChargersTableProps) {
  const [chargers, setChargers] = useState<Charger[]>([])
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    setIsLoading(true)
    const url = stationId
      ? `/api/v1/stations/${stationId}/chargers`
      : "/api/v1/chargers"
    fetch(url)
      .then((r) => r.json())
      .then((body) => { if (!cancelled) setChargers(body.data || []) })
      .catch(() => { if (!cancelled) setChargers([]) })
      .finally(() => { if (!cancelled) setIsLoading(false) })
    return () => { cancelled = true }
  }, [refreshKey, stationId])

  if (isLoading) return <div className="h-48 animate-pulse rounded-xl bg-gray-100" />
  if (chargers.length === 0) return <div className="flex items-center justify-center rounded-xl border border-dashed border-gray-300 p-12"><p className="text-sm text-gray-500">No chargers found</p></div>

  return (
    <ScrollableTable>
      <table className="w-full text-left text-sm">
        <thead>
          <tr className="border-b border-gray-200 text-xs font-medium uppercase text-gray-500">
            <th className="px-4 py-3">ID</th>
            <th className="px-4 py-3">Station</th>
            <th className="px-4 py-3">Connector Type</th>
            <th className="px-4 py-3">Power (kW)</th>
            <th className="px-4 py-3">Current Type</th>
            <th className="px-4 py-3">Status</th>
            <th className="px-4 py-3" />
          </tr>
        </thead>
        <tbody>
          {chargers.map((c) => (
            <tr key={c.id} className="border-b border-gray-100 hover:bg-gray-50">
              <td className="px-4 py-3 font-mono text-xs text-gray-600">{c.id}</td>
              <td className="px-4 py-3 text-gray-900">{c.station_name}</td>
              <td className="px-4 py-3 text-gray-700">{c.connector_type_name}</td>
              <td className="px-4 py-3 text-gray-700">{c.power_kw}</td>
              <td className="px-4 py-3 text-gray-700">{c.current_type}</td>
              <td className="px-4 py-3">
                <span className={`inline-block rounded-full px-2 py-0.5 text-xs font-medium ${statusColors[c.status] || "bg-gray-100 text-gray-600"}`}>
                  {c.status}
                </span>
              </td>
              <td className="px-4 py-3">
                <button onClick={() => onDelete({ id: c.id, label: `${c.station_name} - ${c.connector_type_name}` })} className="text-xs font-medium text-red-600 hover:text-red-800">Delete</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </ScrollableTable>
  )
}
