import { useEffect, useState } from "react"

interface ChargerFormModalProps {
  stationId: string
  onClose: () => void
  onSaved: () => void
}

interface ConnectorType {
  id: string
  name: string
}

export function ChargerFormModal({ stationId, onClose, onSaved }: ChargerFormModalProps) {
  const [connectorTypes, setConnectorTypes] = useState<ConnectorType[]>([])
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetch("/api/v1/connector-types")
      .then((r) => r.json())
      .then((body) => setConnectorTypes(body.data || []))
      .catch(() => setConnectorTypes([]))
  }, [])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    const form = e.target as HTMLFormElement
    const data = new FormData(form)
    const body: Record<string, string | number> = {}
    data.forEach((v, k) => { body[k] = v as string })

    const res = await fetch(`/api/v1/stations/${stationId}/chargers`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })

    if (!res.ok) {
      const errBody = await res.json().catch(() => ({ error: "Request failed" }))
      setError(errBody.error || "Failed to create charger")
      return
    }
    onSaved()
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="w-full max-w-lg rounded-2xl bg-white p-6 shadow-float">
        <h3 className="text-lg font-semibold text-gray-900">Add Charger</h3>
        {error && <div className="mt-3 rounded-lg bg-red-50 p-3 text-sm text-red-700">{error}</div>}
        <form onSubmit={handleSubmit} className="mt-4 space-y-4">
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">Connector Type</label>
            <select name="connector_type_id" required className="block w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent">
              <option value="">Select type...</option>
              {connectorTypes.map((ct) => <option key={ct.id} value={ct.id}>{ct.name}</option>)}
            </select>
          </div>
          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <label className="mb-1 block text-sm font-medium text-gray-700">Power (kW)</label>
              <input name="power_kw" type="number" step="any" required className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent" />
            </div>
            <div>
              <label className="mb-1 block text-sm font-medium text-gray-700">Current Type</label>
              <select name="current_type" required className="block w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent">
                <option value="AC">AC</option>
                <option value="DC">DC</option>
              </select>
            </div>
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">Status</label>
            <select name="status" required className="block w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent">
              <option value="available">Available</option>
              <option value="occupied">Occupied</option>
              <option value="faulted">Faulted</option>
              <option value="offline">Offline</option>
            </select>
          </div>
          <div className="flex justify-end gap-3">
            <button type="button" onClick={onClose} className="rounded-lg border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50">Cancel</button>
            <button type="submit" className="rounded-lg bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent/90">Add</button>
          </div>
        </form>
      </div>
    </div>
  )
}
