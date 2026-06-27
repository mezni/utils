import { type ReactNode } from 'react'
import { Navigate, useLocation } from 'react-router-dom'
import { useAuthStore } from '@/stores/auth-store'

interface AuthGuardProps {
  children: ReactNode
  requiredRoles?: string[]
  redirectTo?: string
}

export function AuthGuard({ children, requiredRoles = [], redirectTo = '/login' }: AuthGuardProps) {
  const { isAuthenticated, hasRole, hasAnyRole, hasAllRoles, isLoading } = useAuthStore()
  const location = useLocation()

  if (isLoading) {
    return <div>Loading...</div> // or a proper loading component
  }

  if (!isAuthenticated) {
    return <Navigate to={redirectTo} state={{ from: location }} replace />
  }

  // Check role requirements
  if (requiredRoles.length > 0) {
    if (requiredRoles.length === 1) {
      if (!hasRole(requiredRoles[0])) {
        return <Navigate to="/unauthorized" replace />
      }
    } else if (!hasAnyRole(requiredRoles)) {
      return <Navigate to="/unauthorized" replace />
    }
  }

  return <>{children}</>
}

export function AdminGuard({ children }: { children: ReactNode }) {
  return <AuthGuard requiredRoles={['ADMIN']}>{children}</AuthGuard>
}

export function PartnerGuard({ children }: { children: ReactNode }) {
  return <AuthGuard requiredRoles={['ADMIN', 'PARTNER']}>{children}</AuthGuard>
}

export function DriverGuard({ children }: { children: ReactNode }) {
  return <AuthGuard requiredRoles={['ADMIN', 'PARTNER', 'DRIVER']}>{children}</AuthGuard>
}

export function GuestGuard({ children }: AuthGuardProps) {
  const isAuthenticated = useAuthStore((s) => s.isAuthenticated)

  if (isAuthenticated) {
    return <Navigate to="/" replace />
  }

  return <>{children}</>
}
