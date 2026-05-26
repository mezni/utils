import { useEffect, useState } from "react"
import { MetricChip } from "@bornemap/ui"
import { BaseMap } from "../map/base-map"

interface Station {
  id: string
  name: string
  city: string
  latitude: number
  longitude: number
  is_operational: boolean
}

interface DashboardData {
  stations: Station[]
  stationCount: number
  chargerCount: number
  partnerCount: number
}

export function OverviewDashboard() {
  const [data, setData] = useState<DashboardData | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false

    async function fetchData() {
      setIsLoading(true)
      setError(null)

      try {
        const [stationsRes, chargersRes, partnersRes] = await Promise.all([
          fetch("/api/v1/stations?limit=100"),
          fetch("/api/v1/chargers?limit=0"),
          fetch("/api/v1/partners?limit=0"),
        ])

        if (!stationsRes.ok) throw new Error("Failed to load stations")
        if (!chargersRes.ok) throw new Error("Failed to load chargers")
        if (!partnersRes.ok) throw new Error("Failed to load partners")

        const stationsBody = await stationsRes.json()
        const chargersBody = await chargersRes.json()
        const partnersBody = await partnersRes.json()

        if (!cancelled) {
          setData({
            stations: stationsBody.data || [],
            stationCount: stationsBody.total ?? stationsBody.data?.length ?? 0,
            chargerCount: chargersBody.total ?? 0,
            partnerCount: partnersBody.total ?? 0,
          })
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "An unexpected error occurred")
        }
      } finally {
        if (!cancelled) {
          setIsLoading(false)
        }
      }
    }

    fetchData()
    return () => { cancelled = true }
  }, [])

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">Overview</h1>
        <p className="mt-1 text-sm text-gray-500">Platform operational dashboard</p>
      </div>

      <div className="mb-8 grid gap-4 sm:grid-cols-3">
        <MetricChip
          label="Total Stations"
          value={data?.stationCount ?? null}
          isLoading={isLoading}
        />
        <MetricChip
          label="Total Chargers"
          value={data?.chargerCount ?? null}
          isLoading={isLoading}
        />
        <MetricChip
          label="Total Partners"
          value={data?.partnerCount ?? null}
          isLoading={isLoading}
        />
      </div>

      {error && (
        <div className="mb-6 rounded-xl border border-red-200 bg-red-50 p-4 text-sm text-red-700">
          {error}
          <button
            onClick={() => window.location.reload()}
            className="ml-2 underline hover:no-underline"
          >
            Retry
          </button>
        </div>
      )}

      <div className="mb-8 overflow-hidden rounded-xl border border-gray-200">
        {isLoading ? (
          <div className="h-[400px] w-full animate-pulse bg-gray-100" />
        ) : (
          <BaseMap
            stations={(data?.stations ?? []).map((s) => ({
              id: s.id,
              name: s.name,
              city: s.city,
              coordinates: [s.latitude, s.longitude],
              chargerCount: 0,
            }))}
          />
        )}
      </div>

      <div className="grid gap-6 md:grid-cols-2">
        <div className="rounded-2xl border border-dashed border-gray-300 bg-white p-6 shadow-card">
          <h3 className="text-sm font-medium text-gray-900">Analytics</h3>
          <p className="mt-1 text-xs text-gray-500">
            Usage trends and performance metrics
          </p>
          <p className="mt-4 text-sm text-gray-400">
            Analytics dashboard coming in a future release.
          </p>
        </div>
        <div className="rounded-2xl border border-dashed border-gray-300 bg-white p-6 shadow-card">
          <h3 className="text-sm font-medium text-gray-900">Geographic Coverage</h3>
          <p className="mt-1 text-xs text-gray-500">
            Station density and coverage heatmap
          </p>
          <p className="mt-4 text-sm text-gray-400">
            Coverage analytics coming in a future release.
          </p>
        </div>
      </div>
    </div>
  )
}
