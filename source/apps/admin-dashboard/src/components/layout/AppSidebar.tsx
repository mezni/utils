import { useState } from "react";
import { NavLink, useLocation } from "react-router-dom";
import { cn } from "@/lib/utils";
import { ROUTES } from "@/lib/constants";
import { ChevronDown, LayoutDashboard, Database, Settings } from "lucide-react";
import type { LucideIcon } from "lucide-react";

const iconMap: Record<string, LucideIcon> = {
  LayoutDashboard,
  Database,
  Settings,
};

interface SidebarItem {
  label: string;
  path?: string;
  icon?: string;
  children?: { label: string; path: string }[];
}

const items: SidebarItem[] = [
  { label: "Dashboard", path: ROUTES.DASHBOARD, icon: "LayoutDashboard" },
  {
    label: "Data",
    icon: "Database",
    children: [
      { label: "Partners", path: ROUTES.PARTNERS },
      { label: "Stations", path: ROUTES.STATIONS },
      { label: "Chargers", path: ROUTES.CHARGERS },
    ],
  },
  { label: "Settings", path: ROUTES.SETTINGS, icon: "Settings" },
];

function SidebarNavItem({ item }: { item: SidebarItem }) {
  const location = useLocation();
  const isActive = item.path
    ? location.pathname === item.path
    : item.children?.some((c) => location.pathname === c.path);
  const [open, setOpen] = useState(isActive);

  if (item.children) {
    return (
      <div>
        <button
          onClick={() => setOpen(!open)}
          className={cn(
            "flex w-full items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors",
            "text-sidebar-foreground/70 hover:bg-sidebar-muted hover:text-sidebar-foreground",
          )}
        >
          {item.icon && (
            <span className="flex h-5 w-5 items-center justify-center">
              {(() => {
                const Icon = iconMap[item.icon];
                return Icon ? <Icon className="h-4 w-4" /> : null;
              })()}
            </span>
          )}
          <span className="flex-1 text-left">{item.label}</span>
          <ChevronDown
            className={cn("h-4 w-4 transition-transform", open && "rotate-180")}
          />
        </button>
        {open && (
          <div className="ml-2 mt-1 space-y-1 pl-6 border-l border-sidebar-muted">
            {item.children.map((child) => (
              <NavLink
                key={child.path}
                to={child.path}
                className={({ isActive }) =>
                  cn(
                    "block rounded-md px-3 py-1.5 text-sm transition-colors",
                    isActive
                      ? "bg-sidebar-active text-white font-medium"
                      : "text-sidebar-foreground/60 hover:text-sidebar-foreground hover:bg-sidebar-muted",
                  )
                }
              >
                {child.label}
              </NavLink>
            ))}
          </div>
        )}
      </div>
    );
  }

  return (
    <NavLink
      to={item.path!}
      className={({ isActive }) =>
        cn(
          "flex items-center gap-3 rounded-lg px-3 py-2 text-sm font-medium transition-colors",
          isActive
            ? "bg-sidebar-active text-white"
            : "text-sidebar-foreground/70 hover:bg-sidebar-muted hover:text-sidebar-foreground",
        )
      }
    >
      {item.icon && (
        <span className="flex h-5 w-5 items-center justify-center">
          {(() => {
            const Icon = iconMap[item.icon];
            return Icon ? <Icon className="h-4 w-4" /> : null;
          })()}
        </span>
      )}
      {item.label}
    </NavLink>
  );
}

export function AppSidebar() {
  return (
    <aside className="flex w-60 flex-col border-r bg-sidebar text-sidebar-foreground">
      <nav className="flex-1 space-y-1 p-3">
        {items.map((item) => (
          <SidebarNavItem key={item.label} item={item} />
        ))}
      </nav>
      <div className="border-t border-sidebar-muted p-3">
        <p className="text-xs text-sidebar-foreground/40">BorneMap v1.0</p>
      </div>
    </aside>
  );
}
