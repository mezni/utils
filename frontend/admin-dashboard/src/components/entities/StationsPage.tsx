import { useState, useEffect, useCallback } from "react";
import { DataTable } from "../ui/DataTable";
import { Card } from "../ui/Card";
import { Badge } from "../ui/Badge";
import { Button } from "../ui/Button";
import { ConfirmAction } from "../ui/ConfirmAction";
import type { Station } from "../../types";
import { stationsApi } from "../../api/stations";
import type { UseToastResult } from "../ui/Toast";
import { Plus, Search, MapPin, Zap, Edit, Trash2, Power } from "lucide-react";

interface StationsPageProps {
  toast: UseToastResult;
  partnerId?: string;
}

export function StationsPage({ toast, partnerId }: StationsPageProps) {
  const [stations, setStations] = useState<Station[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState("");

  const fetchStations = useCallback(async () => {
    try {
      const data = partnerId
        ? await stationsApi.listByPartner(partnerId)
        : await stationsApi.list();
      setStations(data);
    } catch {
      toast.toast("error", "Failed to load stations");
    } finally {
      setLoading(false);
    }
  }, [toast, partnerId]);

  useEffect(() => {
    fetchStations();
  }, [fetchStations]);

  const filtered = stations.filter((s) =>
    s.name.toLowerCase().includes(search.toLowerCase()) ||
    s.address.toLowerCase().includes(search.toLowerCase())
  );

  const handleDelete = async (id: string) => {
    try {
      await stationsApi.delete(id);
      setStations((prev) => prev.filter((s) => s.id !== id));
      toast.toast("success", "Station deleted");
    } catch {
      toast.toast("error", "Failed to delete station");
    }
  };

  const columns = [
    {
      key: "name",
      label: "Station Name",
      render: (value: string, record: Station) => (
        <div className="flex items-center gap-3">
          <div className="w-9 h-9 bg-emerald-100 rounded-lg flex items-center justify-center flex-shrink-0">
            <MapPin size={16} className="text-emerald-600" />
          </div>
          <div>
            <div className="font-medium text-gray-900">{value}</div>
            <div className="text-xs text-gray-500">{record.address}</div>
          </div>
        </div>
      ),
    },
    {
      key: "location",
      label: "Coordinates",
      render: (_: unknown, record: Station) => (
        <div className="text-sm text-gray-600 font-mono">
          {record.latitude.toFixed(4)}, {record.longitude.toFixed(4)}
        </div>
      ),
    },
    {
      key: "partner_id",
      label: "Partner",
      render: (value: string) => (
        <Badge variant="secondary">{value.slice(0, 8)}</Badge>
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
      render: (_: unknown, record: Station) => (
        <div className="flex items-center gap-1 justify-end">
          <Button variant="ghost" size="sm" onClick={() => console.log("Edit station", record.id)}>
            <Edit size={14} />
          </Button>
          <ConfirmAction
            title="Delete Station"
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
          <h2 className="text-2xl font-bold text-gray-900">Stations</h2>
          <p className="text-sm text-gray-500 mt-1">
            {partnerId ? "Partner's stations" : "Manage all charging stations"}
          </p>
        </div>
        <Button onClick={() => console.log("Add station")}>
          <Plus size={16} />
          Add Station
        </Button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
        <Card>
          <div className="p-5">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500">Total Stations</p>
                <p className="text-2xl font-bold text-gray-900 mt-1">{stations.length}</p>
              </div>
              <div className="w-11 h-11 bg-emerald-100 rounded-xl flex items-center justify-center">
                <MapPin size={22} className="text-emerald-600" />
              </div>
            </div>
          </div>
        </Card>
        <Card>
          <div className="p-5">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500">Total Connectors</p>
                <p className="text-2xl font-bold text-gray-900 mt-1">&mdash;</p>
              </div>
              <div className="w-11 h-11 bg-blue-100 rounded-xl flex items-center justify-center">
                <Power size={22} className="text-blue-600" />
              </div>
            </div>
          </div>
        </Card>
        <Card>
          <div className="p-5">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-medium text-gray-500">Power Output</p>
                <p className="text-2xl font-bold text-gray-900 mt-1">&mdash;</p>
              </div>
              <div className="w-11 h-11 bg-amber-100 rounded-xl flex items-center justify-center">
                <Zap size={22} className="text-amber-600" />
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
                placeholder="Search stations..."
                className="w-full pl-10 pr-4 py-2.5 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-emerald-500 focus:border-emerald-500 transition-all"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
              />
            </div>
            <Badge variant="secondary">{filtered.length} stations</Badge>
          </div>
        </div>
      </Card>

      <Card>
        <DataTable
          columns={columns}
          data={filtered}
          loading={loading}
          emptyMessage="No stations found"
        />
      </Card>
    </div>
  );
}
