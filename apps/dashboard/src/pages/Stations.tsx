import DataTable from "../components/DataTable";
import { formatDate } from "../lib/utils";
import { mockStations, type Station } from "../lib/constants";

const columns = [
  { key: "id", header: "ID" },
  { key: "name", header: "Name" },
  { key: "partnerName", header: "Partner" },
  { key: "address", header: "Address", className: "max-w-xs truncate" },
  {
    key: "location",
    header: "Coordinates",
    render: (s: Station) => (
      <span className="text-gray-500 text-xs">
        {s.location.lat.toFixed(4)}, {s.location.lng.toFixed(4)}
      </span>
    ),
  },
  {
    key: "chargerCount",
    header: "Chargers",
    className: "text-right",
  },
  {
    key: "createdAt",
    header: "Created",
    render: (s: Station) => (
      <span className="text-gray-500">{formatDate(s.createdAt)}</span>
    ),
  },
];

export default function Stations() {
  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-100">Stations</h1>
          <p className="text-gray-500 mt-1">Manage charging station locations</p>
        </div>
        <button className="btn btn-primary">
          + Add Station
        </button>
      </div>
      <div className="card overflow-hidden">
        <div className="p-4 border-b border-border">
          <div className="flex items-center gap-2">
            <input
              type="text"
              placeholder="Search stations..."
              className="bg-surface-dark border border-border rounded-lg px-3 py-1.5 text-sm text-gray-300 placeholder-gray-600 focus:outline-none focus:border-primary/50 w-64"
            />
            <button className="btn btn-ghost text-sm">Filter</button>
          </div>
        </div>
        <div className="p-4">
          <DataTable columns={columns} data={mockStations} />
        </div>
        <div className="px-4 py-3 border-t border-border flex items-center justify-between">
          <span className="text-sm text-gray-500">
            Showing {mockStations.length} stations
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
