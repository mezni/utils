import { ScrollableTable } from "@bornemap/ui"
import { useEffect, useState } from "react"

interface Partner {
  id: string
  display_name: string
  classification: string
  tax_id: string | null
  contact_phone: string
  created_at: string
}

interface PartnersTableProps {
  refreshKey: number
  onDelete: (target: { id: string; label: string }) => void
}

export function PartnersTable({ refreshKey, onDelete }: PartnersTableProps) {
  const [partners, setPartners] = useState<Partner[]>([])
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    setIsLoading(true)
    fetch("/api/v1/partners")
      .then((r) => r.json())
      .then((body) => { if (!cancelled) setPartners(body.data || []) })
      .catch(() => { if (!cancelled) setPartners([]) })
      .finally(() => { if (!cancelled) setIsLoading(false) })
    return () => { cancelled = true }
  }, [refreshKey])

  if (isLoading) {
    return <div className="h-48 animate-pulse rounded-xl bg-gray-100" />
  }

  if (partners.length === 0) {
    return (
      <div className="flex items-center justify-center rounded-xl border border-dashed border-gray-300 p-12">
        <p className="text-sm text-gray-500">No partners found</p>
      </div>
    )
  }

  return (
    <ScrollableTable>
      <table className="w-full text-left text-sm">
        <thead>
          <tr className="border-b border-gray-200 text-xs font-medium uppercase text-gray-500">
            <th className="px-4 py-3">ID</th>
            <th className="px-4 py-3">Display Name</th>
            <th className="px-4 py-3">Classification</th>
            <th className="px-4 py-3">Tax ID</th>
            <th className="px-4 py-3">Contact Phone</th>
            <th className="px-4 py-3">Created</th>
            <th className="px-4 py-3" />
          </tr>
        </thead>
        <tbody>
          {partners.map((p) => (
            <tr key={p.id} className="border-b border-gray-100 hover:bg-gray-50">
              <td className="px-4 py-3 font-mono text-xs text-gray-600">{p.id}</td>
              <td className="px-4 py-3 font-medium text-gray-900">{p.display_name}</td>
              <td className="px-4 py-3 text-gray-700">{p.classification}</td>
              <td className="px-4 py-3 text-gray-700">{p.tax_id || "—"}</td>
              <td className="px-4 py-3 text-gray-700">{p.contact_phone}</td>
              <td className="px-4 py-3 text-gray-500">{new Date(p.created_at).toLocaleDateString()}</td>
              <td className="px-4 py-3">
                <button
                  onClick={() => onDelete({ id: p.id, label: p.display_name })}
                  className="text-xs font-medium text-red-600 hover:text-red-800"
                >
                  Delete
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </ScrollableTable>
  )
}
