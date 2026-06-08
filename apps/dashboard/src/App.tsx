import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import AppShell from './components/AppShell';
import OverviewPage from './pages/OverviewPage';
import PartnersPage from './pages/PartnersPage';
import StationsPage from './pages/StationsPage';
import ChargersPage from './pages/ChargersPage';

export default function App() {
  return (
    <BrowserRouter>
      <AppShell>
        <Routes>
          <Route path="/overview" element={<OverviewPage />} />
          <Route path="/partners" element={<PartnersPage />} />
          <Route path="/stations" element={<StationsPage />} />
          <Route path="/chargers" element={<ChargersPage />} />
          <Route path="*" element={<Navigate to="/overview" replace />} />
        </Routes>
      </AppShell>
    </BrowserRouter>
  );
}
