import { useState } from 'react'
import { DashboardLayout } from '@/components/layout/DashboardLayout'
import { Overview } from '@/pages/Overview'
import { Users } from '@/pages/Users'
import { Stations } from '@/pages/Stations'
import { Analytics } from '@/pages/Analytics'
import { System } from '@/pages/System'
import { AuditLog } from '@/pages/AuditLog'
import { Keycloak } from '@/pages/Keycloak'

const pages: Record<string, React.FC> = {
  overview: Overview,
  users: Users,
  stations: Stations,
  analytics: Analytics,
  system: System,
  audit: AuditLog,
  keycloak: Keycloak,
}

export default function App() {
  const [active, setActive] = useState('overview')
  const Page = pages[active] || Overview

  return (
    <DashboardLayout active={active} onNavigate={setActive}>
      <Page />
    </DashboardLayout>
  )
}
