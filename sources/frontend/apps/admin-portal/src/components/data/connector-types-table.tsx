import { ScrollableTable } from "@bornemap/ui"
import { useEffect, useState } from "react"

interface ConnectorType {
  id: string
  name: string
  description: string | null
  created_at: string
}

interface ConnectorTypesTableProps {
  refreshKey: number
  onDelete: (target: { id: string; label: string }) => void
}

export function ConnectorTypesTable({ refreshKey, onDelete }: ConnectorTypesTableProps) {
  const [types, setTypes] = useState<ConnectorType[]>([])
  const [isLoading, setIsLoading] = useState(true)

  useEffect(() => {
    let cancelled = false
    setIsLoading(true)
    fetch("/api/v1/connector-types")
      .then((r) => r.json())
      .then((body) => { if (!cancelled) setTypes(body.data || []) })
      .catch(() => { if (!cancelled) setTypes([]) })
      .finally(() => { if (!cancelled) setIsLoading(false) })
    return () => { cancelled = true }
  }, [refreshKey])

  if (isLoading) return <div className="h-48 animate-pulse rounded-xl bg-gray-100" />
  if (types.length === 0) return <div className="flex items-center justify-center rounded-xl border border-dashed border-gray-300 p-12"><p className="text-sm text-gray-500">No connector types found</p></div>

  return (
    <ScrollableTable>
      <table className="w-full text-left text-sm">
        <thead>
          <tr className="border-b border-gray-200 text-xs font-medium uppercase text-gray-500">
            <th className="px-4 py-3">ID</th>
            <th className="px-4 py-3">Name</th>
            <th className="px-4 py-3">Description</th>
            <th className="px-4 py-3">Created</th>
            <th className="px-4 py-3" />
          </tr>
        </thead>
        <tbody>
          {types.map((t) => (
            <tr key={t.id} className="border-b border-gray-100 hover:bg-gray-50">
              <td className="px-4 py-3 font-mono text-xs text-gray-600">{t.id}</td>
              <td className="px-4 py-3 font-medium text-gray-900">{t.name}</td>
              <td className="px-4 py-3 text-gray-700">{t.description || "—"}</td>
              <td className="px-4 py-3 text-gray-500">{new Date(t.created_at).toLocaleDateString("fr-TN")}</td>
              <td className="px-4 py-3">
                <button onClick={() => onDelete({ id: t.id, label: t.name })} className="text-xs font-medium text-red-600 hover:text-red-800">Delete</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </ScrollableTable>
  )
}
