import { Component, type ReactNode } from "react"
import { Routes, Route, Navigate } from "react-router-dom"
import { AppShell } from "./components/layout/app-shell"
import { AuthInterceptor } from "./services/auth-interceptor"
import { Overview } from "./pages/overview"
import { Stations } from "./pages/stations"
import { Chargers } from "./pages/chargers"
import { Profile } from "./pages/profile"

class ErrorBoundary extends Component<{ children: ReactNode }, { hasError: boolean }> {
  constructor(props: { children: ReactNode }) {
    super(props)
    this.state = { hasError: false }
  }

  static getDerivedStateFromError() {
    return { hasError: true }
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="flex h-full items-center justify-center p-8">
          <div className="rounded-lg border border-red-200 bg-red-50 p-6 text-center">
            <p className="text-sm font-medium text-red-600">Something went wrong</p>
            <button
              onClick={() => { this.setState({ hasError: false }); window.location.reload() }}
              className="mt-3 text-sm font-medium text-red-700 underline"
            >
              Reload page
            </button>
          </div>
        </div>
      )
    }
    return this.props.children
  }
}

const adminRoutes = ["/users", "/settings", "/analytics", "/security"]

function RouteGuard({ children }: { children: ReactNode }) {
  if (adminRoutes.some((route) => window.location.pathname.startsWith(route))) {
    return <Navigate to="/" replace />
  }
  return <>{children}</>
}

export default function App() {
  return (
    <AuthInterceptor>
      <ErrorBoundary>
        <Routes>
          <Route element={<AppShell />}>
            <Route index element={<Overview />} />
            <Route path="stations" element={<Stations />} />
            <Route path="stations/:id/chargers" element={<Chargers />} />
            <Route path="chargers" element={<Chargers />} />
            <Route path="profile" element={<Profile />} />
            <Route path="*" element={<RouteGuard><Navigate to="/" replace /></RouteGuard>} />
          </Route>
        </Routes>
      </ErrorBoundary>
    </AuthInterceptor>
  )
}
