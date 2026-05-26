import { useLocation, useNavigate } from "react-router-dom"

interface NavItem {
  label: string
  route: string
  icon: string
}

const navItems: NavItem[] = [
  { label: "Overview", route: "/", icon: "chart-bar" },
  { label: "Stations", route: "/stations", icon: "map-pin" },
  { label: "Chargers", route: "/chargers", icon: "zap" },
  { label: "Profile", route: "/profile", icon: "user" },
]

const icons: Record<string, JSX.Element> = {
  "chart-bar": (
    <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <line x1="18" y1="20" x2="18" y2="10" /><line x1="12" y1="20" x2="12" y2="4" /><line x1="6" y1="20" x2="6" y2="14" />
    </svg>
  ),
  "map-pin": (
    <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 10c0 7-9 13-9 13s-9-6-9-13a9 9 0 0 1 18 0z" /><circle cx="12" cy="10" r="3" />
    </svg>
  ),
  zap: (
    <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polygon points="13 2 3 14 12 14 11 22 21 10 12 10 13 2" />
    </svg>
  ),
  user: (
    <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2" /><circle cx="12" cy="7" r="4" />
    </svg>
  ),
}

export function SidebarNav() {
  const location = useLocation()
  const navigate = useNavigate()

  return (
    <aside className="flex w-64 flex-col border-r border-gray-200 bg-white">
      <div className="flex h-16 items-center gap-3 border-b border-gray-200 px-6">
        <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-accent">
          <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 text-white" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
          </svg>
        </div>
        <span className="text-lg font-bold text-gray-900">BorneMap</span>
      </div>
      <nav className="flex-1 space-y-1 p-4">
        {navItems.map((item) => {
          const isActive = item.route === "/"
            ? location.pathname === "/"
            : location.pathname.startsWith(item.route)
          return (
            <button
              key={item.route}
              onClick={() => navigate(item.route)}
              className={`flex w-full items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-colors ${
                isActive
                  ? "bg-accent-muted text-accent-dark"
                  : "text-gray-600 hover:bg-gray-100 hover:text-gray-900"
              }`}
            >
              <span className={isActive ? "text-accent" : "text-gray-400"}>
                {icons[item.icon]}
              </span>
              {item.label}
            </button>
          )
        })}
      </nav>
      <div className="border-t border-gray-200 p-4">
        <p className="text-xs text-gray-400">BorneMap v0.1.0</p>
      </div>
    </aside>
  )
}
