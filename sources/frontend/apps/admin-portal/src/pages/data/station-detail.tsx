import { useParams } from "react-router-dom"
import { ChargersTable } from "../../components/data/chargers-table"
import { ChargerFormModal } from "../../components/data/charger-form-modal"
import { ConfirmDeleteModal } from "@bornemap/ui"
import { useState } from "react"

export function StationDetailPage() {
  const { id } = useParams<{ id: string }>()
  const [refreshKey, setRefreshKey] = useState(0)
  const [deleteTarget, setDeleteTarget] = useState<{ id: string; label: string } | null>(null)
  const [showCreate, setShowCreate] = useState(false)

  const handleDelete = async () => {
    if (!deleteTarget) return
    await fetch(`/api/v1/chargers/${deleteTarget.id}`, { method: "DELETE" })
    setDeleteTarget(null)
    setRefreshKey((k) => k + 1)
  }

  return (
    <div>
      <div className="mb-6 flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-gray-900">Station Chargers</h2>
          <p className="text-sm text-gray-500">Station ID: {id}</p>
        </div>
        <button
          onClick={() => setShowCreate(true)}
          className="rounded-lg bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent/90"
        >
          Add Charger
        </button>
      </div>
      <ChargersTable refreshKey={refreshKey} onDelete={setDeleteTarget} stationId={id} />
      {showCreate && (
        <ChargerFormModal
          stationId={id!}
          onClose={() => setShowCreate(false)}
          onSaved={() => { setShowCreate(false); setRefreshKey((k) => k + 1) }}
        />
      )}
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
