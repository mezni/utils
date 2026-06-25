import { useState } from "react";
import { PageHeader } from "@/components/common/PageHeader";
import { DataTable } from "@/components/common/DataTable";
import { SearchInput } from "@/components/common/SearchInput";
import { Button } from "@/components/ui/button";
import { ConfirmDialog } from "@/components/common/ConfirmDialog";
import { StationFormDialog } from "./StationFormDialog";
import { useStations, useCreateStation, useUpdateStation, useDeleteStation } from "@/hooks/use-stations-query";
import type { AdminStationDto, CreateStationRequest, UpdateStationRequest } from "@bornemap/domain-types";
import { Plus, Pencil, Trash2 } from "lucide-react";

export function StationsPage() {
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState("");
  const [formOpen, setFormOpen] = useState(false);
  const [editingStation, setEditingStation] = useState<AdminStationDto | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<AdminStationDto | null>(null);

  const { data, isLoading, isError, error, refetch } = useStations({ page });
  const createMutation = useCreateStation();
  const updateMutation = useUpdateStation(editingStation?.station_id || "");
  const deleteMutation = useDeleteStation();

  const handleCreate = (payload: CreateStationRequest | UpdateStationRequest) => {
    createMutation.mutate(payload as CreateStationRequest, {
      onSuccess: () => { setFormOpen(false); setEditingStation(null); },
    });
  };

  const handleUpdate = (payload: CreateStationRequest | UpdateStationRequest) => {
    if (!editingStation) return;
    updateMutation.mutate(payload as UpdateStationRequest, {
      onSuccess: () => { setFormOpen(false); setEditingStation(null); },
    });
  };

  const handleDelete = () => {
    if (!deleteTarget) return;
    deleteMutation.mutate(deleteTarget.station_id, {
      onSuccess: () => setDeleteTarget(null),
    });
  };

  const filtered = search
    ? data?.data.filter((s) =>
        s.name.toLowerCase().includes(search.toLowerCase()) ||
        s.station_id.toLowerCase().includes(search.toLowerCase()),
      )
    : data?.data;

  return (
    <div>
      <PageHeader
        title="Stations"
        description="Manage charging stations."
        action={
          <Button onClick={() => { setEditingStation(null); setFormOpen(true); }}>
            <Plus className="h-4 w-4 mr-2" />
            Add Station
          </Button>
        }
      />
      <div className="mb-4">
        <SearchInput value={search} onChange={(v) => { setSearch(v); setPage(1); }} placeholder="Search stations..." />
      </div>
      <DataTable
        columns={[
          { key: "station_id", header: "ID", render: (s: AdminStationDto) => <span className="font-mono text-xs">{s.station_id}</span> },
          { key: "name", header: "Name", render: (s: AdminStationDto) => s.name },
          { key: "address", header: "Address", render: (s: AdminStationDto) => s.address || "—" },
          { key: "partner_id", header: "Partner", render: (s: AdminStationDto) => s.partner_id || "—" },
          { key: "lat", header: "Lat", render: (s: AdminStationDto) => s.lat.toFixed(4) },
          { key: "lon", header: "Lon", render: (s: AdminStationDto) => s.lon.toFixed(4) },
          {
            key: "actions",
            header: "",
            render: (s: AdminStationDto) => (
              <div className="flex items-center gap-1">
                <Button variant="ghost" size="sm" onClick={() => { setEditingStation(s); setFormOpen(true); }}>
                  <Pencil className="h-4 w-4" />
                </Button>
                <Button variant="ghost" size="sm" onClick={() => setDeleteTarget(s)}>
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
        emptyMessage="No stations found."
        emptyAction={
          <Button onClick={() => { setEditingStation(null); setFormOpen(true); }}>
            <Plus className="h-4 w-4 mr-2" />
            Add Station
          </Button>
        }
        page={page}
        totalPages={data?.pagination.total_pages}
        onPageChange={setPage}
      />
      <StationFormDialog
        open={formOpen}
        onOpenChange={(o) => { setFormOpen(o); if (!o) setEditingStation(null); }}
        onSubmit={editingStation ? handleUpdate : handleCreate}
        station={editingStation}
        isLoading={createMutation.isPending || updateMutation.isPending}
      />
      <ConfirmDialog
        open={!!deleteTarget}
        onOpenChange={() => setDeleteTarget(null)}
        title="Delete Station"
        description={`Are you sure you want to delete "${deleteTarget?.name}"?`}
        onConfirm={handleDelete}
        isLoading={deleteMutation.isPending}
      />
    </div>
  );
}
