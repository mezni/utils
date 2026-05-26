import { useState, useEffect } from "react"

interface Charger {
  id: string
  station_id: string
  connector_type_id: string
  power_kw: number
  current_type: string
  status: string
}

interface ChargerFormModalProps {
  charger?: Charger
  stationId?: string
  onClose: () => void
  onSaved: () => void
}

export function ChargerFormModal({ charger, stationId, onClose, onSaved }: ChargerFormModalProps) {
  const [connectorTypeId, setConnectorTypeId] = useState(charger?.connector_type_id ?? "")
  const [selectedStationId, setSelectedStationId] = useState(charger?.station_id ?? stationId ?? "")
  const [powerKw, setPowerKw] = useState(charger?.power_kw.toString() ?? "")
  const [currentType, setCurrentType] = useState(charger?.current_type ?? "AC")
  const [status, setStatus] = useState(charger?.status ?? "available")
  const [stations, setStations] = useState<{ id: string; name: string }[]>([])
  const [connectorTypes, setConnectorTypes] = useState<{ id: string; name: string }[]>([])
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [loadingOptions, setLoadingOptions] = useState(true)

  useEffect(() => {
    Promise.all([
      fetch("/api/v1/stations").then((r) => r.json()).then((j) => {
        const data = j.data || j
        setStations(data.map((s: { id: string; name: string }) => ({ id: s.id, name: s.name })))
      }),
      fetch("/api/v1/connector-types").then((r) => r.json()).then((j) => {
        const data = j.data || j
        setConnectorTypes(data.map((c: { id: string; name: string }) => ({ id: c.id, name: c.name })))
      }),
    ]).finally(() => setLoadingOptions(false))
  }, [])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setSaving(true)
    setError(null)

    const kw = parseFloat(powerKw)
    if (isNaN(kw) || kw <= 0) {
      setError("Power must be a positive number")
      setSaving(false)
      return
    }

    const body = { connector_type_id: connectorTypeId, power_kw: kw, current_type: currentType, status }

    try {
      let url: string
      let method: string

      if (charger) {
        url = `/api/v1/stations/${charger.station_id}/chargers/${charger.id}`
        method = "PATCH"
      } else {
        url = `/api/v1/stations/${selectedStationId}/chargers`
        method = "POST"
      }

      const res = await fetch(url, {
        method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      })

      if (!res.ok) {
        const data = await res.json().catch(() => null)
        throw new Error(data?.detail || data?.title || "Failed to save charger")
      }

      onSaved()
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to save charger")
    } finally {
      setSaving(false)
    }
  }

  if (loadingOptions) {
    return (
      <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
        <div className="rounded-2xl bg-white p-6 shadow-xl">
          <p className="text-sm text-gray-400">Loading...</p>
        </div>
      </div>
    )
  }

  const statusOptions = [
    { value: "available", label: "Available" },
    { value: "occupied", label: "Occupied" },
    { value: "faulted", label: "Faulted" },
    { value: "offline", label: "Offline" },
  ]

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="w-full max-w-lg rounded-2xl bg-white p-6 shadow-xl">
        <h3 className="mb-4 text-lg font-semibold text-gray-900">
          {charger ? "Edit Charger" : "Add Charger"}
        </h3>

        <form onSubmit={handleSubmit} className="space-y-4">
          {!charger && !stationId && (
            <div>
              <label className="block text-sm font-medium text-gray-700">Station</label>
              <select
                value={selectedStationId}
                onChange={(e) => setSelectedStationId(e.target.value)}
                required
                className="mt-1 w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
              >
                <option value="">Select a station</option>
                {stations.map((s) => (
                  <option key={s.id} value={s.id}>{s.name}</option>
                ))}
              </select>
            </div>
          )}

          <div>
            <label className="block text-sm font-medium text-gray-700">Connector Type</label>
            <select
              value={connectorTypeId}
              onChange={(e) => setConnectorTypeId(e.target.value)}
              required
              className="mt-1 w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
            >
              <option value="">Select a type</option>
              {connectorTypes.map((ct) => (
                <option key={ct.id} value={ct.id}>{ct.name}</option>
              ))}
            </select>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700">Power (kW)</label>
            <input
              value={powerKw}
              onChange={(e) => setPowerKw(e.target.value)}
              required
              type="number"
              step="0.1"
              className="mt-1 w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
            />
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700">Current Type</label>
              <select
                value={currentType}
                onChange={(e) => setCurrentType(e.target.value)}
                className="mt-1 w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
              >
                <option value="AC">AC</option>
                <option value="DC">DC</option>
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700">Status</label>
              <select
                value={status}
                onChange={(e) => setStatus(e.target.value)}
                className="mt-1 w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
              >
                {statusOptions.map((opt) => (
                  <option key={opt.value} value={opt.value}>{opt.label}</option>
                ))}
              </select>
            </div>
          </div>

          {error && (
            <div className="rounded-md bg-red-50 p-3 text-sm text-red-600">{error}</div>
          )}

          <div className="flex justify-end gap-3 pt-2">
            <button
              type="button"
              onClick={onClose}
              className="rounded-md border border-gray-300 px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={saving}
              className="rounded-md bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent/90 disabled:opacity-50"
            >
              {saving ? "Saving..." : charger ? "Update" : "Create"}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
