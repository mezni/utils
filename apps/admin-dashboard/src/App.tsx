import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ErrorBoundary } from './components/ErrorBoundary';
import { DashboardLayout } from './components/layout/DashboardLayout';
import { DashboardPage } from './pages/Dashboard';
import { PartnersPage } from './pages/Partners';
import { PartnerDetailPage } from './pages/PartnerDetail';
import { StationsPage } from './pages/Stations';
import { StationDetailPage } from './pages/StationDetail';
import { ChargersPage } from './pages/Chargers';
import { ChargerDetailPage } from './pages/ChargerDetail';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      staleTime: 0,
      refetchOnWindowFocus: false,
    },
  },
});

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <ErrorBoundary>
        <BrowserRouter>
          <Routes>
            <Route path="/" element={<DashboardLayout />}>
              <Route index element={<DashboardPage />} />
              <Route path="partners" element={<PartnersPage />} />
              <Route path="partners/:id" element={<PartnerDetailPage />} />
              <Route path="stations" element={<StationsPage />} />
              <Route path="stations/:id" element={<StationDetailPage />} />
              <Route path="chargers" element={<ChargersPage />} />
              <Route path="chargers/:id" element={<ChargerDetailPage />} />
              <Route path="*" element={<Navigate to="/" replace />} />
            </Route>
          </Routes>
        </BrowserRouter>
      </ErrorBoundary>
    </QueryClientProvider>
  );
}

export default App;
