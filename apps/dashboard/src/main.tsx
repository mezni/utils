import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import { BrowserRouter, Routes, Route } from 'react-router-dom'
import { RoleProvider } from './context/RoleContext'
import { AppShell } from './components/AppShell/AppShell'
import { OverviewScreen } from './screens/OverviewScreen.tsx'
import { MyStationsScreen } from './screens/MyStationsScreen.tsx'
import { StationEditScreen } from './screens/StationEditScreen.tsx'
import { ChargerManagementScreen } from './screens/ChargerManagementScreen.tsx'
import { AvailabilityUpdateScreen } from './screens/AvailabilityUpdateScreen.tsx'
import { ReportsScreen } from './screens/ReportsScreen.tsx'
import { UsersScreen } from './screens/UsersScreen.tsx'
import { PartnersScreen } from './screens/PartnersScreen.tsx'
import { StationsScreen } from './screens/StationsScreen.tsx'
import { ChargersScreen } from './screens/ChargersScreen.tsx'
import { ReviewsScreen } from './screens/ReviewsScreen.tsx'
import './index.css'

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <BrowserRouter>
      <RoleProvider>
        <Routes>
          <Route path="/" element={<AppShell />}>
            <Route index element={<OverviewScreen />} />
            <Route path="stations" element={<MyStationsScreen />} />
            <Route path="stations/:id/edit" element={<StationEditScreen />} />
            <Route path="chargers" element={<ChargerManagementScreen />} />
            <Route path="availability" element={<AvailabilityUpdateScreen />} />
            <Route path="reports" element={<ReportsScreen />} />
            <Route path="users" element={<UsersScreen />} />
            <Route path="partners" element={<PartnersScreen />} />
            <Route path="admin/stations" element={<StationsScreen />} />
            <Route path="admin/chargers" element={<ChargersScreen />} />
            <Route path="admin/reviews" element={<ReviewsScreen />} />
          </Route>
        </Routes>
      </RoleProvider>
    </BrowserRouter>
  </StrictMode>
)