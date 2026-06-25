import { useState } from "react";
import { PageHeader } from "@/components/common/PageHeader";
import { DataTable } from "@/components/common/DataTable";
import { SearchInput } from "@/components/common/SearchInput";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { ConfirmDialog } from "@/components/common/ConfirmDialog";
import { PartnerFormDialog } from "./PartnerFormDialog";
import { usePartners, useCreatePartner, useUpdatePartner, useDeletePartner } from "@/hooks/use-partners-query";
import type { AdminPartnerDto, CreatePartnerRequest, UpdatePartnerRequest } from "@bornemap/domain-types";
import { Plus, Pencil, Trash2 } from "lucide-react";

export function PartnersPage() {
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState("");
  const [formOpen, setFormOpen] = useState(false);
  const [editingPartner, setEditingPartner] = useState<AdminPartnerDto | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<AdminPartnerDto | null>(null);

  const { data, isLoading, isError, error, refetch } = usePartners({ page, search });
  const createMutation = useCreatePartner();
  const updateMutation = useUpdatePartner(editingPartner?.partner_id || "");
  const deleteMutation = useDeletePartner();

  const handleCreate = (payload: CreatePartnerRequest | UpdatePartnerRequest) => {
    createMutation.mutate(payload as CreatePartnerRequest, {
      onSuccess: () => {
        setFormOpen(false);
        setEditingPartner(null);
      },
    });
  };

  const handleUpdate = (payload: CreatePartnerRequest | UpdatePartnerRequest) => {
    if (!editingPartner) return;
    updateMutation.mutate(payload as UpdatePartnerRequest, {
      onSuccess: () => {
        setFormOpen(false);
        setEditingPartner(null);
      },
    });
  };

  const handleDelete = () => {
    if (!deleteTarget) return;
    deleteMutation.mutate(deleteTarget.partner_id, {
      onSuccess: () => setDeleteTarget(null),
    });
  };

  return (
    <div>
      <PageHeader
        title="Partners"
        description="Manage EV charging partners."
        action={
          <Button onClick={() => { setEditingPartner(null); setFormOpen(true); }}>
            <Plus className="h-4 w-4 mr-2" />
            Add Partner
          </Button>
        }
      />
      <div className="mb-4">
        <SearchInput
          value={search}
          onChange={(v) => { setSearch(v); setPage(1); }}
          placeholder="Search partners..."
        />
      </div>
      <DataTable
        columns={[
          { key: "partner_id", header: "ID", render: (p: AdminPartnerDto) => <span className="font-mono text-xs">{p.partner_id}</span> },
          { key: "name", header: "Name", render: (p: AdminPartnerDto) => p.name },
          { key: "partner_type", header: "Type", render: (p: AdminPartnerDto) => p.partner_type || "—" },
          { key: "support_email", header: "Email", render: (p: AdminPartnerDto) => p.support_email || "—" },
          {
            key: "is_verified",
            header: "Status",
            render: (p: AdminPartnerDto) => (
              <Badge variant={p.is_verified ? "success" : "warning"}>
                {p.is_verified ? "Verified" : "Pending"}
              </Badge>
            ),
          },
          {
            key: "actions",
            header: "",
            render: (p: AdminPartnerDto) => (
              <div className="flex items-center gap-1">
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => { setEditingPartner(p); setFormOpen(true); }}
                >
                  <Pencil className="h-4 w-4" />
                </Button>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setDeleteTarget(p)}
                >
                  <Trash2 className="h-4 w-4 text-destructive" />
                </Button>
              </div>
            ),
          },
        ]}
        data={data?.data}
        isLoading={isLoading}
        isError={isError}
        error={error}
        onRetry={() => refetch()}
        emptyMessage="No partners found."
        emptyAction={
          <Button onClick={() => { setEditingPartner(null); setFormOpen(true); }}>
            <Plus className="h-4 w-4 mr-2" />
            Add Partner
          </Button>
        }
        page={page}
        totalPages={data?.pagination.total_pages}
        onPageChange={setPage}
      />
      <PartnerFormDialog
        open={formOpen}
        onOpenChange={(o) => { setFormOpen(o); if (!o) setEditingPartner(null); }}
        onSubmit={editingPartner ? handleUpdate : handleCreate}
        partner={editingPartner}
        isLoading={createMutation.isPending || updateMutation.isPending}
      />
      <ConfirmDialog
        open={!!deleteTarget}
        onOpenChange={() => setDeleteTarget(null)}
        title="Delete Partner"
        description={`Are you sure you want to delete "${deleteTarget?.name}"? This action can be undone.`}
        onConfirm={handleDelete}
        isLoading={deleteMutation.isPending}
      />
    </div>
  );
}
