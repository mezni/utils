import { useState } from 'react'
import { Outlet } from 'react-router-dom'
import { Sidebar } from './Sidebar'
import { Header } from './Header'

export function AppLayout() {
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false)

  return (
    <div className="min-h-screen bg-background text-foreground font-body">
      <Sidebar
        collapsed={sidebarCollapsed}
        onToggle={() => setSidebarCollapsed((p) => !p)}
      />

      <div
        className={`transition-all duration-200 ${
          sidebarCollapsed ? 'ml-16' : 'ml-60'
        }`}
      >
        <Header />
        <main
          className="p-6"
          role="main"
          aria-label="Main content"
        >
          <Outlet />
        </main>
      </div>
    </div>
  )
}
