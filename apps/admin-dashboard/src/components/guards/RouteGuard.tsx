import { type ReactNode } from 'react'
import { Navigate, useLocation } from 'react-router-dom'
import { useAuthStore } from '@/stores/auth-store'

interface RouteGuardProps {
  children: ReactNode
  requiredRoles?: string[]
  redirectTo?: string
  publicRoute?: boolean
}

export function RouteGuard({ 
  children, 
  requiredRoles = [], 
  redirectTo = '/login',
  publicRoute = false 
}: RouteGuardProps) {
  const { isAuthenticated, hasRole, hasAnyRole, isLoading } = useAuthStore()
  const location = useLocation()

  if (isLoading) {
    return <div className="flex items-center justify-center min-h-screen">
      <div className="animate-spin rounded-full h-32 w-32 border-b-2 border-gray-900"></div>
    </div>
  }

  // Handle public routes (login, register, etc.)
  if (publicRoute) {
    if (isAuthenticated) {
      // If authenticated and trying to access public route, redirect to dashboard
      return <Navigate to="/" replace />
    }
    return <>{children}</>
  }

  // Protected routes - require authentication
  if (!isAuthenticated) {
    return <Navigate to={redirectTo} state={{ from: location }} replace />
  }

  // Check role requirements for protected routes
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

// Specific route guards for different role levels
export function AdminRoute({ children }: { children: ReactNode }) {
  return (
    <RouteGuard requiredRoles={['ADMIN']}>
      {children}
    </RouteGuard>
  )
}

export function PartnerRoute({ children }: { children: ReactNode }) {
  return (
    <RouteGuard requiredRoles={['ADMIN', 'PARTNER']}>
      {children}
    </RouteGuard>
  )
}

export function DriverRoute({ children }: { children: ReactNode }) {
  return (
    <RouteGuard requiredRoles={['ADMIN', 'PARTNER', 'DRIVER']}>
      {children}
    </RouteGuard>
  )
}

export function PublicRoute({ children }: { children: ReactNode }) {
  return <RouteGuard publicRoute>{children}</RouteGuard>
}

// 403 Unauthorized page
export function UnauthorizedPage() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50">
      <div className="max-w-md w-full text-center">
        <div className="mb-4">
          <svg className="mx-auto h-12 w-12 text-red-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
          </svg>
        </div>
        <h1 className="text-3xl font-bold text-gray-900 mb-2">403 Unauthorized</h1>
        <p className="text-gray-600 mb-4">
          You don't have permission to access this resource.
        </p>
        <button
          onClick={() => window.history.back()}
          className="bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600 transition-colors"
        >
          Go Back
        </button>
      </div>
    </div>
  )
}