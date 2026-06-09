import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { RoleProvider } from './context/RoleContext';
import { AppShell } from './components/layout/AppShell';
import { OverviewPage } from './pages/Overview/OverviewPage';
import { PartnersPage } from './pages/Partners/PartnersPage';
import { StationsPage } from './pages/Stations/StationsPage';
import { ChargersPage } from './pages/Chargers/ChargersPage';

function DashboardRoutes() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={<OverviewPage />} />
        <Route path="partners" element={<PartnersPage />} />
        <Route path="stations" element={<StationsPage />} />
        <Route path="chargers" element={<ChargersPage />} />
      </Route>
    </Routes>
  );
}

export default function App() {
  return (
    <BrowserRouter>
      <RoleProvider>
        <DashboardRoutes />
      </RoleProvider>
    </BrowserRouter>
  );
}
