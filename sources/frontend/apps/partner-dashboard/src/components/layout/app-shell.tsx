import { Outlet } from "react-router-dom"
import { SidebarNav } from "./sidebar-nav"

export function AppShell() {
  return (
    <div className="flex h-screen bg-surface">
      <SidebarNav />
      <div className="flex flex-1 flex-col overflow-hidden">
        <main className="flex-1 overflow-y-auto p-6">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
