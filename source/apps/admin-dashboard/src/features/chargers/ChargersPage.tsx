import { useState } from "react";
import { PageHeader } from "@/components/common/PageHeader";
import { DataTable } from "@/components/common/DataTable";
import { SearchInput } from "@/components/common/SearchInput";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ConfirmDialog } from "@/components/common/ConfirmDialog";
import { ChargerFormDialog } from "./ChargerFormDialog";
import { useChargers, useCreateCharger, useUpdateCharger, useDeleteCharger } from "@/hooks/use-chargers-query";
import type { AdminChargerDto, CreateChargerRequest, UpdateChargerRequest } from "@bornemap/domain-types";
import { Plus, Pencil, Trash2 } from "lucide-react";

const statusLabels: Record<number, { label: string; variant: "success" | "warning" | "destructive" | "secondary" }> = {
  1: { label: "Active", variant: "success" },
  2: { label: "Inactive", variant: "secondary" },
  3: { label: "Maintenance", variant: "warning" },
  4: { label: "Offline", variant: "destructive" },
};

export function ChargersPage() {
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState("");
  const [formOpen, setFormOpen] = useState(false);
  const [editingCharger, setEditingCharger] = useState<AdminChargerDto | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<AdminChargerDto | null>(null);

  const { data, isLoading, isError, error, refetch } = useChargers({ page });
  const createMutation = useCreateCharger();
  const updateMutation = useUpdateCharger(editingCharger?.charger_id || "");
  const deleteMutation = useDeleteCharger();

  const handleCreate = (payload: CreateChargerRequest | UpdateChargerRequest) => {
    createMutation.mutate(payload as CreateChargerRequest, {
      onSuccess: () => { setFormOpen(false); setEditingCharger(null); },
    });
  };

  const handleUpdate = (payload: CreateChargerRequest | UpdateChargerRequest) => {
    if (!editingCharger) return;
    updateMutation.mutate(payload as UpdateChargerRequest, {
      onSuccess: () => { setFormOpen(false); setEditingCharger(null); },
    });
  };

  const handleDelete = () => {
    if (!deleteTarget) return;
    deleteMutation.mutate(deleteTarget.charger_id, {
      onSuccess: () => setDeleteTarget(null),
    });
  };

  const filtered = search
    ? data?.data.filter((c) =>
        c.charger_id.toLowerCase().includes(search.toLowerCase()) ||
        c.station_id.toLowerCase().includes(search.toLowerCase()),
      )
    : data?.data;

  return (
    <div>
      <PageHeader
        title="Chargers"
        description="Manage EV chargers."
        action={
          <Button onClick={() => { setEditingCharger(null); setFormOpen(true); }}>
            <Plus className="h-4 w-4 mr-2" />
            Add Charger
          </Button>
        }
      />
      <div className="mb-4">
        <SearchInput value={search} onChange={(v) => { setSearch(v); setPage(1); }} placeholder="Search chargers..." />
      </div>
      <DataTable
        columns={[
          { key: "charger_id", header: "ID", render: (c: AdminChargerDto) => <span className="font-mono text-xs">{c.charger_id}</span> },
          { key: "station_id", header: "Station", render: (c: AdminChargerDto) => <span className="font-mono text-xs">{c.station_id}</span> },
          { key: "connector_type_id", header: "Connector", render: (c: AdminChargerDto) => `Type ${c.connector_type_id}` },
          {
            key: "status_id",
            header: "Status",
            render: (c: AdminChargerDto) => {
              const s = statusLabels[c.status_id] || { label: "Unknown", variant: "secondary" as const };
              return <Badge variant={s.variant}>{s.label}</Badge>;
            },
          },
          { key: "power_kw", header: "Power", render: (c: AdminChargerDto) => c.power_kw ? `${c.power_kw} kW` : "—" },
          { key: "count_available", header: "Avail", render: (c: AdminChargerDto) => `${c.count_available}/${c.count_total}` },
          {
            key: "actions",
            header: "",
            render: (c: AdminChargerDto) => (
              <div className="flex items-center gap-1">
                <Button variant="ghost" size="sm" onClick={() => { setEditingCharger(c); setFormOpen(true); }}>
                  <Pencil className="h-4 w-4" />
                </Button>
                <Button variant="ghost" size="sm" onClick={() => setDeleteTarget(c)}>
                  <Trash2 className="h-4 w-4 text-destructive" />
                </Button>
              </div>
            ),
          },
        ]}
        data={filtered}
        isLoading={isLoading}
        isError={isError}
        error={error}
        onRetry={() => refetch()}
        emptyMessage="No chargers found."
        emptyAction={
          <Button onClick={() => { setEditingCharger(null); setFormOpen(true); }}>
            <Plus className="h-4 w-4 mr-2" />
            Add Charger
          </Button>
        }
        page={page}
        totalPages={data?.pagination.total_pages}
        onPageChange={setPage}
      />
      <ChargerFormDialog
        open={formOpen}
        onOpenChange={(o) => { setFormOpen(o); if (!o) setEditingCharger(null); }}
        onSubmit={editingCharger ? handleUpdate : handleCreate}
        charger={editingCharger}
        isLoading={createMutation.isPending || updateMutation.isPending}
      />
      <ConfirmDialog
        open={!!deleteTarget}
        onOpenChange={() => setDeleteTarget(null)}
        title="Delete Charger"
        description={`Are you sure you want to delete charger "${deleteTarget?.charger_id}"?`}
        onConfirm={handleDelete}
        isLoading={deleteMutation.isPending}
      />
    </div>
  );
}
