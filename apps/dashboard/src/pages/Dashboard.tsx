import StatCard from "../components/StatCard";
import { dashboardStats, mockChargers } from "../lib/constants";

const statusColor = {
  available: "badge badge-active",
  occupied: "badge bg-amber-500/10 text-amber-400",
  offline: "badge bg-red-500/10 text-red-400",
  unknown: "badge bg-gray-500/10 text-gray-400",
};

const recentChargers = mockChargers.slice(0, 4);

export default function Dashboard() {
  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-100">Dashboard</h1>
        <p className="text-gray-500 mt-1">Overview of your EV charging network</p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <StatCard
          title="Total Partners"
          value={dashboardStats.totalPartners}
          subtitle={`${dashboardStats.verifiedPartners} verified`}
          icon="●"
          trend={{ value: "12%", positive: true }}
        />
        <StatCard
          title="Total Stations"
          value={dashboardStats.totalStations}
          icon="■"
          trend={{ value: "8%", positive: true }}
        />
        <StatCard
          title="Total Chargers"
          value={dashboardStats.totalChargers}
          icon="▲"
          trend={{ value: "15%", positive: true }}
        />
        <StatCard
          title="Available Chargers"
          value={dashboardStats.availableChargers}
          subtitle={`${dashboardStats.activeChargers} active`}
          icon="●"
          trend={{ value: "5%", positive: false }}
        />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="card p-6">
          <h2 className="text-lg font-semibold text-gray-100 mb-4">Network Status</h2>
          <div className="space-y-3">
            <div className="flex items-center justify-between py-2">
              <span className="text-sm text-gray-400">Available</span>
              <div className="flex items-center gap-2">
                <div className="w-24 h-2 rounded-full bg-surface-light overflow-hidden">
                  <div
                    className="h-full rounded-full bg-accent transition-all"
                    style={{ width: `${(dashboardStats.availableChargers / dashboardStats.totalChargers) * 100}%` }}
                  />
                </div>
                <span className="text-sm font-medium text-gray-300 w-8 text-right">
                  {Math.round((dashboardStats.availableChargers / dashboardStats.totalChargers) * 100)}%
                </span>
              </div>
            </div>
            <div className="flex items-center justify-between py-2">
              <span className="text-sm text-gray-400">Occupied</span>
              <div className="flex items-center gap-2">
                <div className="w-24 h-2 rounded-full bg-surface-light overflow-hidden">
                  <div
                    className="h-full rounded-full bg-amber-400 transition-all"
                    style={{ width: "20%" }}
                  />
                </div>
                <span className="text-sm font-medium text-gray-300 w-8 text-right">20%</span>
              </div>
            </div>
            <div className="flex items-center justify-between py-2">
              <span className="text-sm text-gray-400">Offline</span>
              <div className="flex items-center gap-2">
                <div className="w-24 h-2 rounded-full bg-surface-light overflow-hidden">
                  <div
                    className="h-full rounded-full bg-red-400 transition-all"
                    style={{ width: "20%" }}
                  />
                </div>
                <span className="text-sm font-medium text-gray-300 w-8 text-right">20%</span>
              </div>
            </div>
          </div>
        </div>

        <div className="card p-6">
          <h2 className="text-lg font-semibold text-gray-100 mb-4">Recent Chargers</h2>
          <div className="space-y-3">
            {recentChargers.map((charger) => (
              <div
                key={charger.id}
                className="flex items-center justify-between py-2 border-b border-border/50 last:border-0"
              >
                <div>
                  <p className="text-sm font-medium text-gray-300">{charger.id}</p>
                  <p className="text-xs text-gray-500">{charger.stationName}</p>
                </div>
                <div className="flex items-center gap-3">
                  <span className="text-xs text-gray-500">{charger.powerKw}kW</span>
                  <span className={statusColor[charger.status]}>
                    {charger.status}
                  </span>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
