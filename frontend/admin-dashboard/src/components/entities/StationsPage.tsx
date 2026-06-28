import { useState, useEffect, useCallback } from "react";
import { DataTable } from "../ui/DataTable";
import { CommandBar } from "../ui/CommandBar";
import { SideDrawer } from "../ui/SideDrawer";
import { EntityForm } from "../ui/EntityForm";
import { ConfirmAction } from "../ui/ConfirmAction";
import { Badge, MapPin, PlugZap } from "../ui/Badge";
import type { Station, CreateStationInput, UpdateStationInput, Connector, CreateConnectorInput } from "../../types";
import { stationsApi } from "../../api/stations";
import { connectorsApi } from "../../api/connectors";
import type { UseToastResult } from "../ui/Toast";
import { Pencil, Trash2, Plus } from "lucide-react";

interface StationsPageProps {
  toast: UseToastResult;
  partnerId?: string;
}

export function StationsPage({ toast, partnerId }: StationsPageProps) {
  const [stations, setStations] = useState<Station[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");

  const [drawerOpen, setDrawerOpen] = useState(false);
  const [drawerMode, setDrawerMode] = useState<"create" | "edit" | "detail">("detail");
  const [selectedStation, setSelectedStation] = useState<Station | null>(null);

  const [connectors, setConnectors] = useState<Connector[]>([]);
  const [connectorDrawerOpen, setConnectorDrawerOpen] = useState(false);

  const fetchStations = useCallback(async () => {
    try {
      const data = await stationsApi.list(partnerId);
      setStations(data);
    } catch (err) {
      toast.toast("error", "Failed to load stations");
    } finally {
      setLoading(false);
    }
  }, [partnerId, toast]);

  useEffect(() => {
    fetchStations();
  }, [fetchStations]);

  const fetchConnectors = useCallback(async (stationId: string) => {
    try {
      const data = await connectorsApi.listByStation(stationId);
      setConnectors(data);
    } catch {
      setConnectors([]);
    }
  }, []);

  const filtered = stations.filter((s) =>
    s.name.toLowerCase().includes(search.toLowerCase()) ||
    s.address.toLowerCase().includes(search.toLowerCase())
  );

  const openDetail = (station: Station) => {
    setSelectedStation(station);
    setDrawerMode("detail");
    setDrawerOpen(true);
    fetchConnectors(station.id);
  };

  const openEdit = () => {
    setDrawerMode("edit");
  };

  const openCreate = () => {
    setSelectedStation(null);
    setDrawerMode("create");
    setDrawerOpen(true);
  };

  const handleCreate = async (values: Record<string, string | number>) => {
    const input: CreateStationInput = {
      partner_id: partnerId || "",
      name: values.name as string,
      address: values.address as string,
      latitude: values.latitude as number,
      longitude: values.longitude as number,
    };
    const station = await stationsApi.create(input);
    setStations((prev) => [...prev, station]);
    setDrawerOpen(false);
    toast.toast("success", "Station created");
  };

  const handleUpdate = async (values: Record<string, string | number>) => {
    if (!selectedStation) return;
    const input: UpdateStationInput = {
      name: values.name as string,
      address: values.address as string,
      latitude: values.latitude as number,
      longitude: values.longitude as number,
    };
    const updated = await stationsApi.update(selectedStation.id, input);
    setStations((prev) => prev.map((s) => (s.id === updated.id ? updated : s)));
    setSelectedStation(updated);
    setDrawerMode("detail");
    toast.toast("success", "Station updated");
  };

  const handleDelete = async () => {
    if (!selectedStation) return;
    await stationsApi.delete(selectedStation.id);
    setStations((prev) => prev.filter((s) => s.id !== selectedStation.id));
    setDrawerOpen(false);
    setSelectedStation(null);
    toast.toast("success", "Station deleted");
  };

  const handleAddConnector = async (values: Record<string, string | number>) => {
    if (!selectedStation) return;
    const input: CreateConnectorInput = {
      station_id: selectedStation.id,
      connector_type: values.connector_type as string,
      power_kw: values.power_kw as number,
    };
    const conn = await connectorsApi.create(input);
    setConnectors((prev) => [...prev, conn]);
    setConnectorDrawerOpen(false);
    toast.toast("success", "Connector added");
  };

  const handleDeleteConnector = async (connectorId: string) => {
    await connectorsApi.delete(connectorId);
    setConnectors((prev) => prev.filter((c) => c.id !== connectorId));
    toast.toast("success", "Connector removed");
  };

  const StationDrawerContent = () => {
    if (drawerMode === "create") {
      return (
        <EntityForm
          fields={[
            { name: "name", label: "Station Name", placeholder: "e.g. Downtown Hub", required: true },
            { name: "address", label: "Address", placeholder: "123 Main St", required: true },
            { name: "latitude", label: "Latitude", type: "number", step: "0.000001", required: true },
            { name: "longitude", label: "Longitude", type: "number", step: "0.000001", required: true },
          ]}
          onSubmit={handleCreate}
          onCancel={() => setDrawerOpen(false)}
          submitLabel="Create Station"
        />
      );
    }

    if (drawerMode === "edit" && selectedStation) {
      return (
        <EntityForm
          fields={[
            { name: "name", label: "Station Name", required: true },
            { name: "address", label: "Address", required: true },
            { name: "latitude", label: "Latitude", type: "number", step: "0.000001", required: true },
            { name: "longitude", label: "Longitude", type: "number", step: "0.000001", required: true },
          ]}
          onSubmit={handleUpdate}
          onCancel={() => setDrawerMode("detail")}
          submitLabel="Save Changes"
        />
      );
    }

    if (drawerMode === "detail" && selectedStation) {
      return (
        <div className="space-y-6">
          <div className="flex items-center justify-between">
            <h3 className="text-base font-semibold text-surface-50">{selectedStation.name}</h3>
            <div className="flex items-center gap-2">
              <button onClick={openEdit} className="btn-ghost p-1.5 rounded-md" title="Edit">
                <Pencil size={15} />
              </button>
              <ConfirmAction
                title="Delete Station"
                message={`Are you sure you want to delete "${selectedStation.name}"? This action cannot be undone.`}
                onConfirm={handleDelete}
                trigger={(open: () => void) => (
                  <button onClick={open} className="btn-ghost p-1.5 rounded-md text-danger-400 hover:text-danger-400" title="Delete">
                    <Trash2 size={15} />
                  </button>
                )}
              />
            </div>
          </div>

          <div className="space-y-3">
            <div>
              <span className="text-xs text-surface-500 uppercase tracking-wider font-medium">Address</span>
              <p className="text-sm text-surface-50 mt-0.5 flex items-center gap-1.5">
                <MapPin size={14} className="text-surface-400" />
                {selectedStation.address}
              </p>
            </div>
            <div className="grid grid-cols-2 gap-4">
              <div>
                <span className="text-xs text-surface-500 uppercase tracking-wider font-medium">Latitude</span>
                <p className="text-sm text-surface-50 mt-0.5">{selectedStation.latitude.toFixed(6)}</p>
              </div>
              <div>
                <span className="text-xs text-surface-500 uppercase tracking-wider font-medium">Longitude</span>
                <p className="text-sm text-surface-50 mt-0.5">{selectedStation.longitude.toFixed(6)}</p>
              </div>
            </div>
            <div>
              <span className="text-xs text-surface-500 uppercase tracking-wider font-medium">Created</span>
              <p className="text-sm text-surface-50 mt-0.5">{new Date(selectedStation.created_at).toLocaleString()}</p>
            </div>
          </div>

          <div className="border-t border-surface-700/50 pt-6">
            <div className="flex items-center justify-between mb-4">
              <h4 className="text-sm font-semibold text-surface-200 flex items-center gap-2">
                <PlugZap size={15} className="text-brand-400" />
                Connectors ({connectors.length})
              </h4>
              <button onClick={() => setConnectorDrawerOpen(true)} className="btn-ghost text-xs p-1.5 rounded-md">
                <Plus size={15} />
              </button>
            </div>

            {connectors.length === 0 ? (
              <p className="text-sm text-surface-500 text-center py-6">No connectors yet</p>
            ) : (
              <div className="space-y-2">
                {connectors.map((c) => (
                  <div key={c.id} className="flex items-center justify-between px-3 py-2.5 rounded-lg bg-surface-800 border border-surface-700/50">
                    <div className="flex items-center gap-3">
                      <Badge variant="brand">{c.connector_type}</Badge>
                      <span className="text-sm text-surface-50">{c.power_kw} kW</span>
                    </div>
                    <ConfirmAction
                      title="Remove Connector"
                      message={`Remove this ${c.connector_type} connector?`}
                      onConfirm={() => handleDeleteConnector(c.id)}
                      trigger={(open: () => void) => (
                        <button onClick={open} className="btn-ghost p-1 rounded-md text-danger-400 opacity-0 group-hover:opacity-100">
                          <Trash2 size={14} />
                        </button>
                      )}
                    />
                  </div>
                ))}
              </div>
            )}
          </div>

          <SideDrawer
            open={connectorDrawerOpen}
            onClose={() => setConnectorDrawerOpen(false)}
            title="Add Connector"
          >
            <EntityForm
              fields={[
                {
                  name: "connector_type",
                  label: "Connector Type",
                  type: "select",
                  required: true,
                  options: [
                    { value: "CCS", label: "CCS (Combined Charging System)" },
                    { value: "CHAdeMO", label: "CHAdeMO" },
                    { value: "Type2", label: "Type 2 (AC)" },
                    { value: "Type1", label: "Type 1 (J1772)" },
                    { value: "Tesla", label: "Tesla Supercharger" },
                  ],
                },
                {
                  name: "power_kw",
                  label: "Power Output (kW)",
                  type: "number",
                  step: "0.1",
                  min: 1,
                  required: true,
                },
              ]}
              onSubmit={handleAddConnector}
              onCancel={() => setConnectorDrawerOpen(false)}
              submitLabel="Add Connector"
            />
          </SideDrawer>
        </div>
      );
    }

    return null;
  };

  return (
    <>
      <CommandBar
        onCreateLabel="Create Station"
        onCreate={openCreate}
        searchValue={search}
        onSearchChange={setSearch}
        searchPlaceholder="Search stations..."
      />

      <DataTable
        loading={loading}
        columns={[
          { key: "name", header: "Name", sortable: true },
          { key: "address", header: "Address", sortable: true },
          {
            key: "latitude",
            header: "Location",
            render: (row: Station) => (
              <span className="text-surface-400 text-xs">
                {row.latitude.toFixed(4)}, {row.longitude.toFixed(4)}
              </span>
            ),
          },
          {
            key: "created_at",
            header: "Created",
            render: (row: Station) => (
              <span className="text-surface-400 text-xs">
                {new Date(row.created_at).toLocaleDateString()}
              </span>
            ),
          },
        ]}
        data={filtered}
        onRowClick={(row) => openDetail(row as unknown as Station)}
      />

      <SideDrawer
        open={drawerOpen}
        onClose={() => { setDrawerOpen(false); setConnectorDrawerOpen(false); }}
        title={
          drawerMode === "create" ? "Create Station" :
          drawerMode === "edit" ? "Edit Station" :
          selectedStation?.name || "Station Details"
        }
      >
        <StationDrawerContent />
      </SideDrawer>
    </>
  );
}
