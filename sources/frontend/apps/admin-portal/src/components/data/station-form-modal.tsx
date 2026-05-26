import { useEffect, useState } from "react"

interface StationFormModalProps {
  onClose: () => void
  onSaved: () => void
}

interface Partner {
  id: string
  display_name: string
}

export function StationFormModal({ onClose, onSaved }: StationFormModalProps) {
  const [partners, setPartners] = useState<Partner[]>([])
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetch("/api/v1/partners")
      .then((r) => r.json())
      .then((body) => setPartners(body.data || []))
      .catch(() => setPartners([]))
  }, [])

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    const form = e.target as HTMLFormElement
    const data = new FormData(form)
    const body: Record<string, string | number> = {}
    data.forEach((v, k) => { body[k] = v as string })

    const res = await fetch("/api/v1/stations", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })

    if (!res.ok) {
      const errBody = await res.json().catch(() => ({ error: "Request failed" }))
      setError(errBody.error || "Failed to create station")
      return
    }
    onSaved()
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="w-full max-w-lg rounded-2xl bg-white p-6 shadow-float">
        <h3 className="text-lg font-semibold text-gray-900">Create Station</h3>
        {error && <div className="mt-3 rounded-lg bg-red-50 p-3 text-sm text-red-700">{error}</div>}
        <form onSubmit={handleSubmit} className="mt-4 space-y-4">
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">Name</label>
            <input name="name" required className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent" />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">Address</label>
            <input name="address" className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent" />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">City</label>
            <input name="city" className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent" />
          </div>
          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <label className="mb-1 block text-sm font-medium text-gray-700">Latitude</label>
              <input name="latitude" type="number" step="any" required className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent" />
            </div>
            <div>
              <label className="mb-1 block text-sm font-medium text-gray-700">Longitude</label>
              <input name="longitude" type="number" step="any" required className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent" />
            </div>
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">Owner</label>
            <select name="owner_id" required className="block w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent">
              <option value="">Select owner...</option>
              {partners.map((p) => <option key={p.id} value={p.id}>{p.display_name}</option>)}
            </select>
          </div>
          <div className="flex items-center gap-2">
            <input name="is_operational" type="checkbox" defaultChecked className="rounded border-gray-300 text-accent focus:ring-accent" />
            <label className="text-sm text-gray-700">Operational</label>
          </div>
          <div className="flex justify-end gap-3">
            <button type="button" onClick={onClose} className="rounded-lg border border-gray-300 bg-white px-4 py-2 text-sm font-medium text-gray-700 hover:bg-gray-50">Cancel</button>
            <button type="submit" className="rounded-lg bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent/90">Create</button>
          </div>
        </form>
      </div>
    </div>
  )
}
