import DataTable from "../components/DataTable";
import { formatDate } from "../lib/utils";
import { mockPartners, type Partner } from "../lib/constants";

const columns = [
  { key: "id", header: "ID" },
  { key: "name", header: "Name" },
  {
    key: "networkType",
    header: "Type",
    render: (p: Partner) => (
      <span className="badge bg-primary/10 text-primary">{p.networkType}</span>
    ),
  },
  { key: "supportPhone", header: "Phone" },
  { key: "supportEmail", header: "Email" },
  {
    key: "isVerified",
    header: "Status",
    render: (p: Partner) => (
      <span className={p.isVerified ? "badge badge-active" : "badge badge-inactive"}>
        {p.isVerified ? "Verified" : "Unverified"}
      </span>
    ),
  },
  {
    key: "stationCount",
    header: "Stations",
    className: "text-right",
  },
  {
    key: "createdAt",
    header: "Created",
    render: (p: Partner) => (
      <span className="text-gray-500">{formatDate(p.createdAt)}</span>
    ),
  },
];

export default function Partners() {
  return (
    <div>
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-gray-100">Partners</h1>
          <p className="text-gray-500 mt-1">Manage charging network operators</p>
        </div>
        <button className="btn btn-primary">
          + Add Partner
        </button>
      </div>
      <div className="card overflow-hidden">
        <div className="p-4 border-b border-border">
          <div className="flex items-center gap-2">
            <input
              type="text"
              placeholder="Search partners..."
              className="bg-surface-dark border border-border rounded-lg px-3 py-1.5 text-sm text-gray-300 placeholder-gray-600 focus:outline-none focus:border-primary/50 w-64"
            />
            <button className="btn btn-ghost text-sm">Filter</button>
          </div>
        </div>
        <div className="p-4">
          <DataTable columns={columns} data={mockPartners} />
        </div>
        <div className="px-4 py-3 border-t border-border flex items-center justify-between">
          <span className="text-sm text-gray-500">
            Showing {mockPartners.length} partners
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
