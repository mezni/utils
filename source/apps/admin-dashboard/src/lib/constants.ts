export const ROUTES = {
  DASHBOARD: "/dashboard",
  PARTNERS: "/data/partners",
  STATIONS: "/data/stations",
  CHARGERS: "/data/chargers",
  SETTINGS: "/settings",
} as const;

export const API_BASE = "/api/v1";

export const SIDEBAR_ITEMS = [
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
] as const;
