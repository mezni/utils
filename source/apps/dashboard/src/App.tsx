import { BrowserRouter, Routes, Route } from 'react-router-dom';
import { RoleProvider, useRole } from './context/RoleContext';
import { AppShell } from './components/layout/AppShell';
import { OverviewPage } from './pages/Overview/OverviewPage';
import { PartnersPage } from './pages/Partners/PartnersPage';
import { StationsPage } from './pages/Stations/StationsPage';
import { ChargersPage } from './pages/Chargers/ChargersPage';
import { PartnerOverviewPage } from './pages/PartnerOverview/PartnerOverviewPage';
import { PartnerStationsPage } from './pages/PartnerStations/PartnerStationsPage';
import { PartnerChargersPage } from './pages/PartnerChargers/PartnerChargersPage';
import { PartnerAvailabilityPage } from './pages/PartnerAvailability/PartnerAvailabilityPage';

function DashboardRoutes() {
  const { role } = useRole();
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={role === 'admin' ? <OverviewPage /> : <PartnerOverviewPage />} />
        <Route path="partners" element={<PartnersPage />} />
        <Route path="stations" element={<StationsPage />} />
        <Route path="chargers" element={<ChargersPage />} />
        <Route path="my-stations" element={<PartnerStationsPage />} />
        <Route path="my-chargers" element={<PartnerChargersPage />} />
        <Route path="availability" element={<PartnerAvailabilityPage />} />
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
