import { Routes, Route, Navigate } from "react-router-dom"
import { AppShell } from "./components/layout/app-shell"
import { OverviewPage } from "./pages/overview"
import { UsersPage } from "./pages/users"
import { DataPage } from "./pages/data"
import { AnalyticsPage } from "./pages/analytics"
import { SecurityPage } from "./pages/security"
import { SettingsPage } from "./pages/settings"

export default function App() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={<OverviewPage />} />
        <Route path="overview" element={<Navigate to="/" replace />} />
        <Route path="users" element={<UsersPage />} />
        <Route path="data" element={<DataPage />} />
        <Route path="analytics" element={<AnalyticsPage />} />
        <Route path="security" element={<SecurityPage />} />
        <Route path="settings" element={<SettingsPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Route>
    </Routes>
  )
}
