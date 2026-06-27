import { type ReactNode } from 'react'
import { useAuthStore } from '@/stores/auth-store'

interface PermissionGateProps {
  children: ReactNode
  requiredRoles?: string[]
  fallback?: ReactNode
  permission?: string
}

export function PermissionGate({ 
  children, 
  requiredRoles = [], 
  fallback = null,
  permission 
}: PermissionGateProps) {
  const { hasRole, hasAnyRole, hasAllRoles, user } = useAuthStore()

  // If no role requirements, show children
  if (requiredRoles.length === 0 && !permission) {
    return <>{children}</>
  }

  // Check permission-based access
  if (permission && user) {
    // This is a placeholder for future permission-based access control
    // For now, we'll just use role-based access
    const hasPermission = true // Implement actual permission checking here
    if (!hasPermission) {
      return <>{fallback}</>
    }
  }

  // Check role-based access
  if (requiredRoles.length > 0) {
    if (requiredRoles.length === 1) {
      if (!hasRole(requiredRoles[0])) {
        return <>{fallback}</>
      }
    } else if (!hasAnyRole(requiredRoles)) {
      return <>{fallback}</>
    }
  }

  return <>{children}</>
}

interface AdminOnlyProps {
  children: ReactNode
  fallback?: ReactNode
}

export function AdminOnly({ children, fallback = null }: AdminOnlyProps) {
  return (
    <PermissionGate requiredRoles={['ADMIN']} fallback={fallback}>
      {children}
    </PermissionGate>
  )
}

interface PartnerOrAdminProps {
  children: ReactNode
  fallback?: ReactNode
}

export function PartnerOrAdmin({ children, fallback = null }: PartnerOrAdminProps) {
  return (
    <PermissionGate requiredRoles={['ADMIN', 'PARTNER']} fallback={fallback}>
      {children}
    </PermissionGate>
  )
}

interface DriverOrHigherProps {
  children: ReactNode
  fallback?: ReactNode
}

export function DriverOrHigher({ children, fallback = null }: DriverOrHigherProps) {
  return (
    <PermissionGate requiredRoles={['ADMIN', 'PARTNER', 'DRIVER']} fallback={fallback}>
      {children}
    </PermissionGate>
  )
}

// Hook for checking permissions in components
export function usePermissions() {
  const { hasRole, hasAnyRole, hasAllRoles, user } = useAuthStore()

  return {
    hasRole,
    hasAnyRole,
    hasAllRoles,
    user,
    isAdmin: user?.role === 'ADMIN',
    isPartner: user?.role === 'PARTNER',
    isDriver: user?.role === 'DRIVER',
    isAuthenticated: !!user,
  }
}