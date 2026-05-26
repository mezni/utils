import { Routes, Route, Navigate } from "react-router-dom"
import { AppShell } from "./components/layout/app-shell"
import { OverviewPage } from "./pages/overview"
import { UsersPage } from "./pages/users"
import { DataPage } from "./pages/data"
import { AnalyticsPage } from "./pages/analytics"
import { SecurityPage } from "./pages/security"
import { SettingsPage } from "./pages/settings"
import { PartnersPage } from "./pages/data/partners"
import { StationsPage } from "./pages/data/stations"
import { ChargersPage } from "./pages/data/chargers"
import { StationDetailPage } from "./pages/data/station-detail"
import { InfrastructureTypesPage } from "./pages/settings/infrastructure-types"
import { AppSettingsPage } from "./pages/settings/app"

export default function App() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={<OverviewPage />} />
        <Route path="overview" element={<Navigate to="/" replace />} />
        <Route path="users" element={<UsersPage />} />
        <Route path="data" element={<DataPage />}>
          <Route index element={<Navigate to="partners" replace />} />
          <Route path="partners" element={<PartnersPage />} />
          <Route path="stations" element={<StationsPage />} />
          <Route path="chargers" element={<ChargersPage />} />
        </Route>
        <Route path="stations/:id/chargers" element={<StationDetailPage />} />
        <Route path="analytics" element={<AnalyticsPage />} />
        <Route path="security" element={<SecurityPage />} />
        <Route path="settings" element={<SettingsPage />}>
          <Route index element={<Navigate to="infrastructure-types" replace />} />
          <Route path="infrastructure-types" element={<InfrastructureTypesPage />} />
          <Route path="app" element={<AppSettingsPage />} />
        </Route>
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  )
}
