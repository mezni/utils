import { createContext, useContext, useState, ReactNode } from 'react'
import { UserRole } from '../types'

interface RoleContextType {
  role: UserRole
  setRole: (role: UserRole) => void
  toggleRole: () => void
}

const RoleContext = createContext<RoleContextType | undefined>(undefined)

export const RoleProvider = ({ children }: { children: ReactNode }) => {
  const [role, setRole] = useState<UserRole>('partner')

  const toggleRole = () => {
    setRole(role === 'partner' ? 'admin' : 'partner')
  }

  return (
    <RoleContext.Provider value={{ role, setRole, toggleRole }}>
      {children}
    </RoleContext.Provider>
  )
}

export const useRole = () => {
  const context = useContext(RoleContext)
  if (!context) {
    throw new Error('useRole must be used within RoleProvider')
  }
  return context
}