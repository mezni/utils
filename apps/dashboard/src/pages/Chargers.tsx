import DataTable from "../components/DataTable";
import { formatDate } from "../lib/utils";
import { mockChargers, type Charger } from "../lib/constants";

const statusBadge: Record<string, string> = {
  available: "badge badge-active",
  occupied: "badge bg-amber-500/10 text-amber-400",
  offline: "badge bg-red-500/10 text-red-400",
  unknown: "badge bg-gray-500/10 text-gray-400",
};

const columns = [
  { key: "id", header: "ID" },
  { key: "stationName", header: "Station" },
  { key: "connectorType", header: "Connector" },
  { key: "currentType", header: "Current" },
  {
    key: "status",
    header: "Status",
    render: (c: Charger) => (
      <span className={statusBadge[c.status]}>{c.status}</span>
    ),
  },
  { key: "powerKw", header: "Power", render: (c: Charger) => `${c.powerKw} kW` },
  { key: "voltage", header: "Voltage", render: (c: Charger) => `${c.voltage}V` },
  { key: "amperage", header: "Amps", render: (c: Charger) => `${c.amperage}A` },
  {
    key: "countAvailable",
    header: "Available",
    className: "text-right",
  },
  {
    key: "countTotal",
    header: "Total",
    className: "text-right",
  },
  {
    key: "createdAt",
    header: "Created",
    render: (c: Charger) => (
      <span className="text-gray-500">{formatDate(c.createdAt)}</span>
    ),
  },
];

export default function Chargers() {
  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-100">Chargers</h1>
          <p className="text-gray-500 mt-1">Manage individual charging units</p>
        </div>
        <button className="btn btn-primary">
          + Add Charger
        </button>
      </div>
      <div className="card overflow-hidden">
        <div className="p-4 border-b border-border">
          <div className="flex items-center gap-2">
            <input
              type="text"
              placeholder="Search chargers..."
              className="bg-surface-dark border border-border rounded-lg px-3 py-1.5 text-sm text-gray-300 placeholder-gray-600 focus:outline-none focus:border-primary/50 w-64"
            />
            <button className="btn btn-ghost text-sm">Filter</button>
          </div>
        </div>
        <div className="p-4">
          <DataTable columns={columns} data={mockChargers} />
        </div>
        <div className="px-4 py-3 border-t border-border flex items-center justify-between">
          <span className="text-sm text-gray-500">
            Showing {mockChargers.length} chargers
          </span>
          <div className="flex items-center gap-2">
            <button className="btn btn-ghost text-xs px-3 py-1" disabled>
              Previous
            </button>
            <button className="btn btn-ghost text-xs px-3 py-1" disabled>
              Next
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
