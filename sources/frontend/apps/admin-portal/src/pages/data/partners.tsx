import { PartnersTable } from "../../components/data/partners-table"
import { PartnerFormModal } from "../../components/data/partner-form-modal"
import { ConfirmDeleteModal } from "@bornemap/ui"
import { useState } from "react"

export function PartnersPage() {
  const [refreshKey, setRefreshKey] = useState(0)
  const [deleteTarget, setDeleteTarget] = useState<{ id: string; label: string } | null>(null)
  const [showCreate, setShowCreate] = useState(false)

  const handleDelete = async () => {
    if (!deleteTarget) return
    await fetch(`/api/v1/partners/${deleteTarget.id}`, { method: "DELETE" })
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
          Create Partner
        </button>
      </div>
      <PartnersTable refreshKey={refreshKey} onDelete={setDeleteTarget} />
      {showCreate && (
        <PartnerFormModal
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
