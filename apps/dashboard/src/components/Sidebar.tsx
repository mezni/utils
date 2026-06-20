import { NavLink } from "react-router-dom";
import { cn } from "../lib/utils";

const navItems = [
  { to: "/", label: "Dashboard", icon: "◆" },
  { to: "/partners", label: "Partners", icon: "●" },
  { to: "/stations", label: "Stations", icon: "■" },
  { to: "/chargers", label: "Chargers", icon: "▲" },
];

export default function Sidebar() {
  return (
    <aside className="fixed left-0 top-0 bottom-0 w-64 bg-surface-dark border-r border-border z-50 flex flex-col">
      <div className="p-6 border-b border-border">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-primary flex items-center justify-center text-white font-bold text-sm">
            B
          </div>
          <div>
            <h1 className="text-sm font-semibold text-gray-100">BorneMap</h1>
            <p className="text-xs text-gray-500">Admin Dashboard</p>
          </div>
        </div>
      </div>

      <nav className="flex-1 p-4 space-y-1">
        {navItems.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === "/"}
            className={({ isActive }) =>
              cn(
                "sidebar-link",
                isActive ? "sidebar-link-active" : "sidebar-link-inactive"
              )
            }
          >
            <span className="w-5 text-center text-sm">{item.icon}</span>
            {item.label}
          </NavLink>
        ))}
      </nav>

      <div className="p-4 border-t border-border">
        <div className="flex items-center gap-3 px-4 py-2">
          <div className="w-8 h-8 rounded-full bg-surface-light flex items-center justify-center text-xs font-medium text-gray-400">
            AD
          </div>
          <div className="flex-1 min-w-0">
            <p className="text-sm font-medium text-gray-300 truncate">Admin User</p>
            <p className="text-xs text-gray-500 truncate">admin@bornemap.io</p>
          </div>
        </div>
      </div>
    </aside>
  );
}
