import { createBrowserRouter, Navigate } from "react-router-dom";
import { AppShell } from "@/components/layout/AppShell";
import { DashboardPage } from "@/features/dashboard/DashboardPage";
import { PartnersPage } from "@/features/partners/PartnersPage";
import { StationsPage } from "@/features/stations/StationsPage";
import { ChargersPage } from "@/features/chargers/ChargersPage";
import { SettingsPage } from "@/features/settings/SettingsPage";
import { ROUTES } from "./constants";

export const router = createBrowserRouter([
  {
    element: <AppShell />,
    children: [
      { index: true, element: <Navigate to={ROUTES.DASHBOARD} replace /> },
      { path: ROUTES.DASHBOARD, element: <DashboardPage /> },
      { path: ROUTES.PARTNERS, element: <PartnersPage /> },
      { path: ROUTES.STATIONS, element: <StationsPage /> },
      { path: ROUTES.CHARGERS, element: <ChargersPage /> },
      { path: ROUTES.SETTINGS, element: <SettingsPage /> },
    ],
  },
]);
