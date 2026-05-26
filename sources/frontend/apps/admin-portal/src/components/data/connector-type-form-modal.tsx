import { useState } from "react"

interface ConnectorTypeFormModalProps {
  onClose: () => void
  onSaved: () => void
}

export function ConnectorTypeFormModal({ onClose, onSaved }: ConnectorTypeFormModalProps) {
  const [error, setError] = useState<string | null>(null)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    const form = e.target as HTMLFormElement
    const data = new FormData(form)
    const body: Record<string, string> = {}
    data.forEach((v, k) => { body[k] = v as string })

    const res = await fetch("/api/v1/connector-types", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })

    if (!res.ok) {
      const errBody = await res.json().catch(() => ({ error: "Request failed" }))
      setError(errBody.error || "Failed to create connector type")
      return
    }
    onSaved()
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="w-full max-w-lg rounded-2xl bg-white p-6 shadow-float">
        <h3 className="text-lg font-semibold text-gray-900">Create Connector Type</h3>
        {error && <div className="mt-3 rounded-lg bg-red-50 p-3 text-sm text-red-700">{error}</div>}
        <form onSubmit={handleSubmit} className="mt-4 space-y-4">
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">Name</label>
            <input name="name" required className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent" />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">Description</label>
            <textarea name="description" rows={3} className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent" />
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
