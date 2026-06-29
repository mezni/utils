import { useState, useEffect, useCallback } from "react";
import { DataTable } from "../ui/DataTable";
import { SideDrawer } from "../ui/SideDrawer";
import { EntityForm } from "../ui/EntityForm";
import { ConfirmAction } from "../ui/ConfirmAction";
import type { Partner, CreatePartnerInput } from "../../types";
import { partnersApi } from "../../api/partners";
import type { UseToastResult } from "../ui/Toast";
import { Button } from "../ui/Button";
import { Card } from "../ui/Card";
import { Badge } from "../ui/Badge";
import { Plus, Search, Edit, Trash2, Building2, UserCheck, TrendingUp } from "lucide-react";

interface PartnersPageProps {
  toast: UseToastResult;
  onNavigate: (page: string, partnerId?: string) => void;
}

export function PartnersPage({ toast, onNavigate }: PartnersPageProps) {
  const [partners, setPartners] = useState<Partner[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [editingPartner, setEditingPartner] = useState<Partner | null>(null);
  const [deletingId, setDeletingId] = useState<string | null>(null);

  const fetchPartners = useCallback(async () => {
    try {
      const data = await partnersApi.list();
      setPartners(data);
    } catch {
      toast.toast("error", "Failed to load partners");
    } finally {
      setLoading(false);
    }
  }, [toast]);

  useEffect(() => {
    fetchPartners();
  }, [fetchPartners]);

  const filtered = partners.filter((p) =>
    p.name.toLowerCase().includes(search.toLowerCase())
  );

  const handleCreate = async (values: Record<string, string | number>) => {
    try {
      const partner = await partnersApi.create({ name: values.name as string });
      setPartners((prev) => [...prev, partner]);
      setDrawerOpen(false);
      toast.toast("success", "Partner created");
    } catch {
      toast.toast("error", "Failed to create partner");
    }
  };

  const handleUpdate = async (values: Record<string, string | number>) => {
    if (!editingPartner) return;
    try {
      const partner = await partnersApi.update(editingPartner.id, values);
      setPartners((prev) => prev.map((p) => (p.id === partner.id ? partner : p)));
      setDrawerOpen(false);
      setEditingPartner(null);
      toast.toast("success", "Partner updated");
    } catch {
      toast.toast("error", "Failed to update partner");
    }
  };

  const handleDelete = async (id: string) => {
    try {
      await partnersApi.delete(id);
      setPartners((prev) => prev.filter((p) => p.id !== id));
      toast.toast("success", "Partner deleted");
    } catch {
      toast.toast("error", "Failed to delete partner");
    }
  };

  const columns = [
    {
      key: "name",
      label: "Partner Name",
      render: (value: string, record: Partner) => (
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 bg-emerald-100 rounded-full flex items-center justify-center flex-shrink-0">
            <Building2 size={16} className="text-emerald-600" />
          </div>
          <div>
            <div className="font-medium text-gray-900">{value}</div>
            <div className="text-xs text-gray-500">{record.id}</div>
          </div>
        </div>
      ),
    },
    {
      key: "created_at",
      label: "Created",
      render: (value: string) => (
        <div className="text-sm text-gray-600">
          {new Date(value).toLocaleDateString()}
        </div>
      ),
    },
    {
      key: "actions",
      label: "",
      render: (_: unknown, record: Partner) => (
        <div className="flex items-center gap-1 justify-end">
          <Button variant="ghost" size="sm" onClick={() => { setEditingPartner(record); setDrawerOpen(true); }}>
            <Edit size={14} />
          </Button>
          <ConfirmAction
            title="Delete Partner"
            message={`Are you sure you want to delete ${record.name}? This action cannot be undone.`}
            confirmLabel="Delete"
            onConfirm={async () => { await handleDelete(record.id); }}
            trigger={(open) => (
              <Button variant="ghost" size="sm" onClick={open}>
                <Trash2 size={14} />
              </Button>
            )}
          />
        </div>
      ),
    },
  ];

  return (
    <div className="space-y-6 animate-fade-in">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold text-gray-900">Partners</h2>
          <p className="text-sm text-gray-500 mt-1">Manage your charging station partners</p>
        </div>
        <Button onClick={() => setDrawerOpen(true)}>
          <Plus size={16} />
          Add Partner
        </Button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <Card>
          <div className="p-5">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500">Total Partners</p>
                <p className="text-2xl font-bold text-gray-900 mt-1">{partners.length}</p>
              </div>
              <div className="w-11 h-11 bg-emerald-100 rounded-xl flex items-center justify-center">
                <Building2 size={22} className="text-emerald-600" />
              </div>
            </div>
          </div>
        </Card>
        <Card>
          <div className="p-5">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500">Active</p>
                <p className="text-2xl font-bold text-gray-900 mt-1">{partners.length}</p>
              </div>
              <div className="w-11 h-11 bg-emerald-100 rounded-xl flex items-center justify-center">
                <UserCheck size={22} className="text-emerald-600" />
              </div>
            </div>
          </div>
        </Card>
        <Card>
          <div className="p-5">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500">New This Month</p>
                <p className="text-2xl font-bold text-gray-900 mt-1">3</p>
              </div>
              <div className="w-11 h-11 bg-amber-100 rounded-xl flex items-center justify-center">
                <TrendingUp size={22} className="text-amber-600" />
              </div>
            </div>
          </div>
        </Card>
      </div>

      <Card>
        <div className="p-5">
          <div className="flex flex-col sm:flex-row gap-4 items-start sm:items-center justify-between">
            <div className="relative flex-1 max-w-md">
              <Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" />
              <input
                type="text"
                placeholder="Search partners..."
                className="w-full pl-10 pr-4 py-2.5 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-emerald-500 focus:border-emerald-500 transition-all"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
              />
            </div>
            <Badge variant="secondary">{filtered.length} partners</Badge>
          </div>
        </div>
      </Card>

      <Card>
        <DataTable
          columns={columns}
          data={filtered}
          loading={loading}
          emptyMessage="No partners found"
        />
      </Card>

      <SideDrawer
        title={editingPartner ? "Edit Partner" : "Add New Partner"}
        open={drawerOpen}
        onClose={() => { setDrawerOpen(false); setEditingPartner(null); }}
      >
        <EntityForm
          fields={[{
            name: "name",
            label: "Partner Name",
            type: "text",
            required: true,
            placeholder: "Enter partner name",
          }]}
          initialData={editingPartner ? { name: editingPartner.name } : {}}
          onSubmit={editingPartner ? handleUpdate : handleCreate}
          onCancel={() => { setDrawerOpen(false); setEditingPartner(null); }}
        />
      </SideDrawer>
    </div>
  );
}
