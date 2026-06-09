import { createContext, useContext, useState, type ReactNode } from 'react';

export type Role = 'admin' | 'partner';

interface RoleContextValue {
  role: Role;
  selectedPartnerId: string | null;
  setRole: (role: Role) => void;
  setSelectedPartnerId: (id: string | null) => void;
}

const RoleContext = createContext<RoleContextValue | null>(null);

export function RoleProvider({ children }: { children: ReactNode }) {
  const [role, setRole] = useState<Role>('admin');
  const [selectedPartnerId, setSelectedPartnerId] = useState<string | null>(null);

  return (
    <RoleContext.Provider value={{ role, selectedPartnerId, setRole, setSelectedPartnerId }}>
      {children}
    </RoleContext.Provider>
  );
}

export function useRole() {
  const ctx = useContext(RoleContext);
  if (!ctx) throw new Error('useRole must be used within RoleProvider');
  return ctx;
}
