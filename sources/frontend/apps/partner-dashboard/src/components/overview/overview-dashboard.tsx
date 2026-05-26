import { useState, useEffect } from "react"
import { MetricChip } from "@bornemap/ui"

export function OverviewDashboard() {
  const [stationCount, setStationCount] = useState<number | null>(null)
  const [chargerCount, setChargerCount] = useState<number | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    Promise.all([
      fetch("/api/v1/stations")
        .then((r) => {
          if (!r.ok) throw new Error("Failed to load stations")
          return r.json()
        })
        .then((j) => {
          const data = j.data || j
          setStationCount(Array.isArray(data) ? data.length : 0)
        }),
      fetch("/api/v1/chargers")
        .then((r) => {
          if (!r.ok) throw new Error("Failed to load chargers")
          return r.json()
        })
        .then((j) => {
          const data = j.data || j
          setChargerCount(Array.isArray(data) ? data.length : 0)
        }),
    ])
      .catch((e) => setError(e instanceof Error ? e.message : "Failed to load dashboard data"))
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div>
        <h2 className="mb-6 text-lg font-semibold text-gray-900">Overview</h2>
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
          <MetricChip label="Total Stations" value={0} isLoading />
          <MetricChip label="Total Chargers" value={0} isLoading />
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div>
        <h2 className="mb-6 text-lg font-semibold text-gray-900">Overview</h2>
        <div className="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-600">
          <p>{error}</p>
          <button onClick={() => window.location.reload()} className="mt-2 text-sm font-medium text-red-700 underline">
            Retry
          </button>
        </div>
      </div>
    )
  }

  return (
    <div>
      <h2 className="mb-6 text-lg font-semibold text-gray-900">Overview</h2>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
        <MetricChip label="Total Stations" value={stationCount ?? 0} />
        <MetricChip label="Total Chargers" value={chargerCount ?? 0} />
      </div>

      {stationCount === 0 && (
        <div className="mt-6 rounded-lg border-2 border-dashed border-gray-200 p-8 text-center text-sm text-gray-400">
          No stations yet. Navigate to Stations to create your first one.
        </div>
      )}
    </div>
  )
}
