import { useState } from "react"

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

interface StationFormModalProps {
  station?: Station
  onClose: () => void
  onSaved: () => void
}

export function StationFormModal({ station, onClose, onSaved }: StationFormModalProps) {
  const [name, setName] = useState(station?.name ?? "")
  const [address, setAddress] = useState("")
  const [city, setCity] = useState(station?.city ?? "")
  const [latitude, setLatitude] = useState(station?.latitude.toString() ?? "")
  const [longitude, setLongitude] = useState(station?.longitude.toString() ?? "")
  const [isOperational, setIsOperational] = useState(station?.is_operational ?? true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setSaving(true)
    setError(null)

    const lat = parseFloat(latitude)
    const lng = parseFloat(longitude)

    if (isNaN(lat) || isNaN(lng)) {
      setError("Invalid coordinates")
      setSaving(false)
      return
    }

    try {
      const url = station
        ? `/api/v1/stations/${station.id}`
        : "/api/v1/stations"
      const method = station ? "PATCH" : "POST"

      const res = await fetch(url, {
        method,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name,
          address,
          city,
          latitude: lat,
          longitude: lng,
          is_operational: isOperational,
        }),
      })

      if (!res.ok) {
        const data = await res.json().catch(() => null)
        throw new Error(data?.detail || data?.title || "Failed to save station")
      }

      onSaved()
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to save station")
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="w-full max-w-lg rounded-2xl bg-white p-6 shadow-xl">
        <h3 className="mb-4 text-lg font-semibold text-gray-900">
          {station ? "Edit Station" : "Create Station"}
        </h3>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gray-700">Name</label>
            <input
              value={name}
              onChange={(e) => setName(e.target.value)}
              required
              className="mt-1 w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
            />
          </div>

          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700">Latitude</label>
              <input
                value={latitude}
                onChange={(e) => setLatitude(e.target.value)}
                required
                type="number"
                step="any"
                className="mt-1 w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Longitude</label>
              <input
                value={longitude}
                onChange={(e) => setLongitude(e.target.value)}
                required
                type="number"
                step="any"
                className="mt-1 w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
              />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700">Address</label>
            <input
              value={address}
              onChange={(e) => setAddress(e.target.value)}
              className="mt-1 w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700">City</label>
            <input
              value={city}
              onChange={(e) => setCity(e.target.value)}
              required
              className="mt-1 w-full rounded-md border border-gray-300 px-3 py-2 text-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
            />
          </div>

          <div className="flex items-center gap-2">
            <input
              type="checkbox"
              checked={isOperational}
              onChange={(e) => setIsOperational(e.target.checked)}
              className="h-4 w-4 rounded border-gray-300 text-accent focus:ring-accent"
            />
            <label className="text-sm font-medium text-gray-700">Operational</label>
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
              {saving ? "Saving..." : station ? "Update" : "Create"}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}
