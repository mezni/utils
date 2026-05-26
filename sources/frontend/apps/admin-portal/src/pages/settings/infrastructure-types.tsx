import { ConnectorTypesTable } from "../../components/data/connector-types-table"
import { ConnectorTypeFormModal } from "../../components/data/connector-type-form-modal"
import { ConfirmDeleteModal } from "@bornemap/ui"
import { useState } from "react"

export function InfrastructureTypesPage() {
  const [refreshKey, setRefreshKey] = useState(0)
  const [deleteTarget, setDeleteTarget] = useState<{ id: string; label: string } | null>(null)
  const [deleteError, setDeleteError] = useState<string | null>(null)
  const [showCreate, setShowCreate] = useState(false)

  const handleDelete = async () => {
    if (!deleteTarget) return
    const res = await fetch(`/api/v1/connector-types/${deleteTarget.id}`, { method: "DELETE" })
    if (res.status === 409) {
      const body = await res.json()
      setDeleteError(body.error || "Cannot delete: type is in use")
      return
    }
    setDeleteTarget(null)
    setRefreshKey((k) => k + 1)
  }

  return (
    <div>
      <div className="mb-4 flex justify-end">
        <button
          onClick={() => setShowCreate(true)}
          className="rounded-lg bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent/90"
        >
          Create Connector Type
        </button>
      </div>
      <ConnectorTypesTable refreshKey={refreshKey} onDelete={setDeleteTarget} />
      {showCreate && (
        <ConnectorTypeFormModal
          onClose={() => setShowCreate(false)}
          onSaved={() => { setShowCreate(false); setRefreshKey((k) => k + 1) }}
        />
      )}
      {deleteError && (
        <div className="mt-4 rounded-xl border border-red-200 bg-red-50 p-4 text-sm text-red-700">
          {deleteError}
          <button onClick={() => setDeleteError(null)} className="ml-2 underline hover:no-underline">Dismiss</button>
        </div>
      )}
      <ConfirmDeleteModal
        isOpen={!!deleteTarget}
        resourceId={deleteTarget?.id ?? ""}
        resourceLabel={deleteTarget?.label ?? ""}
        onConfirm={handleDelete}
        onCancel={() => { setDeleteTarget(null); setDeleteError(null) }}
      />
    </div>
  )
}
