import { useState } from "react"

interface PartnerFormModalProps {
  onClose: () => void
  onSaved: () => void
}

export function PartnerFormModal({ onClose, onSaved }: PartnerFormModalProps) {
  const [classification, setClassification] = useState("Business")
  const [error, setError] = useState<string | null>(null)

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    const form = e.target as HTMLFormElement
    const data = new FormData(form)
    const body: Record<string, string> = {}
    data.forEach((v, k) => { body[k] = v as string })

    const res = await fetch("/api/v1/partners", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    })

    if (!res.ok) {
      const errBody = await res.json().catch(() => ({ error: "Request failed" }))
      setError(errBody.error || "Failed to create partner")
      return
    }
    onSaved()
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="w-full max-w-lg rounded-2xl bg-white p-6 shadow-float">
        <h3 className="text-lg font-semibold text-gray-900">Create Partner</h3>
        {error && (
          <div className="mt-3 rounded-lg bg-red-50 p-3 text-sm text-red-700">{error}</div>
        )}
        <form onSubmit={handleSubmit} className="mt-4 space-y-4">
          <div className="grid gap-4 sm:grid-cols-2">
            <div>
              <label className="mb-1 block text-sm font-medium text-gray-700">Email</label>
              <input name="email" type="email" required className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent" />
            </div>
            <div>
              <label className="mb-1 block text-sm font-medium text-gray-700">Password</label>
              <input name="password" type="password" required className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent" />
            </div>
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">Display Name</label>
            <input name="display_name" required className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent" />
          </div>
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">Classification</label>
            <select
              value={classification}
              onChange={(e) => setClassification(e.target.value)}
              className="block w-full rounded-md border border-gray-300 bg-white px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
            >
              <option value="Business">Business</option>
              <option value="Private">Private</option>
            </select>
          </div>
          {classification === "Business" && (
            <div>
              <label className="mb-1 block text-sm font-medium text-gray-700">Tax ID</label>
              <input name="tax_id" className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent" />
            </div>
          )}
          <div>
            <label className="mb-1 block text-sm font-medium text-gray-700">Contact Phone</label>
            <input name="contact_phone" className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent" />
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
