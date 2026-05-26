import { NavLink, Outlet } from "react-router-dom"

const tabs = [
  { to: "/data/partners", label: "Partners" },
  { to: "/data/stations", label: "Stations" },
  { to: "/data/chargers", label: "Chargers" },
]

export function DataPage() {
  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-900">Data</h1>
        <p className="mt-1 text-sm text-gray-500">Browse and manage stations, chargers, and partners</p>
      </div>
      <div className="mb-6 flex gap-1 rounded-xl bg-gray-100 p-1">
        {tabs.map((tab) => (
          <NavLink
            key={tab.to}
            to={tab.to}
            end
            className={({ isActive }) =>
              `rounded-lg px-4 py-2 text-sm font-medium transition ${
                isActive ? "bg-white text-gray-900 shadow-sm" : "text-gray-600 hover:text-gray-900"
              }`
            }
          >
            {tab.label}
          </NavLink>
        ))}
      </div>
      <Outlet />
    </div>
  )
}
