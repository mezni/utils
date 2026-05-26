import { Outlet } from "react-router-dom"
import { SidebarNav } from "./sidebar-nav"
import { Header } from "./header"
import { SandboxProvider, useSandbox } from "../../context/sandbox-context"

export function AppShell() {
  return (
    <SandboxProvider>
      <AppShellInner />
    </SandboxProvider>
  )
}

function AppShellInner() {
  const { isSandboxActive } = useSandbox()

  return (
    <div className={`flex h-screen bg-surface ${isSandboxActive ? "border-t-4 border-sky-500" : ""}`}>
      <SidebarNav />
      <div className="flex flex-1 flex-col overflow-hidden">
        <Header />
        <main className="flex-1 overflow-y-auto p-6">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
