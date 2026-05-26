import { ChargersTable } from "../../components/data/chargers-table"
import { ConfirmDeleteModal } from "@bornemap/ui"
import { useState } from "react"

export function ChargersPage() {
  const [refreshKey, setRefreshKey] = useState(0)
  const [deleteTarget, setDeleteTarget] = useState<{ id: string; label: string } | null>(null)

  const handleDelete = async () => {
    if (!deleteTarget) return
    await fetch(`/api/v1/chargers/${deleteTarget.id}`, { method: "DELETE" })
    setDeleteTarget(null)
    setRefreshKey((k) => k + 1)
  }

  return (
    <div>
      <ChargersTable refreshKey={refreshKey} onDelete={setDeleteTarget} />
      <ConfirmDeleteModal
        isOpen={!!deleteTarget}
        resourceId={deleteTarget?.id ?? ""}
        resourceLabel={deleteTarget?.label ?? ""}
        onConfirm={handleDelete}
        onCancel={() => setDeleteTarget(null)}
      />
    </div>
  )
}
