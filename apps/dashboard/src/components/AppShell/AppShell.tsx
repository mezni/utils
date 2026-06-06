import { Outlet } from 'react-router-dom'
import { Sidebar } from './Sidebar/Sidebar'
import { TopBar } from './TopBar'

export const AppShell = () => {
  return (
    <div className="flex h-screen">
      <Sidebar />
      <div className="flex-1 flex flex-col min-w-0">
        <TopBar />
        <div className="flex-1 overflow-auto bg-surface-background">
          <Outlet />
        </div>
      </div>
    </div>
  )
}