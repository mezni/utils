import { useState, useEffect, useCallback } from "react";
import { DataTable } from "../ui/DataTable";
import { CommandBar } from "../ui/CommandBar";
import { SideDrawer } from "../ui/SideDrawer";
import { EntityForm } from "../ui/EntityForm";
import type { Partner, CreatePartnerInput } from "../../types";
import { partnersApi } from "../../api/partners";
import type { UseToastResult } from "../ui/Toast";

interface PartnersPageProps {
  toast: UseToastResult;
  onNavigate: (page: string, partnerId?: string) => void;
}

export function PartnersPage({ toast, onNavigate }: PartnersPageProps) {
  const [partners, setPartners] = useState<Partner[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");
  const [drawerOpen, setDrawerOpen] = useState(false);

  const fetchPartners = useCallback(async () => {
    try {
      const data = await partnersApi.list();
      setPartners(data);
    } catch (err) {
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
    const input: CreatePartnerInput = { name: values.name as string };
    const partner = await partnersApi.create(input);
    setPartners((prev) => [...prev, partner]);
    setDrawerOpen(false);
    toast.toast("success", "Partner created");
  };

  return (
    <>
      <CommandBar
        onCreateLabel="Create Partner"
        onCreate={() => setDrawerOpen(true)}
        searchValue={search}
        onSearchChange={setSearch}
        searchPlaceholder="Search partners..."
      />

      <DataTable
        loading={loading}
        columns={[
          { key: "name", header: "Name", sortable: true },
          {
            key: "created_at",
            header: "Created",
            render: (row) => (
              <span className="text-surface-400 text-xs">
                {new Date(row.created_at).toLocaleDateString()}
              </span>
            ),
          },
        ]}
        data={filtered}
        onRowClick={(row) => onNavigate("stations", row.id)}
      />

      <SideDrawer
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        title="Create Partner"
      >
        <EntityForm
          fields={[
            {
              name: "name",
              label: "Partner Name",
              placeholder: "e.g. ChargePoint Inc.",
              required: true,
            },
          ]}
          onSubmit={handleCreate}
          onCancel={() => setDrawerOpen(false)}
          submitLabel="Create Partner"
        />
      </SideDrawer>
    </>
  );
}
