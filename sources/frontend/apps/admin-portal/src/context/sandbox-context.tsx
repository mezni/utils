import { createContext, useContext, useState, useEffect, type ReactNode } from "react"

const SANDBOX_STORAGE_KEY = "bornemap_admin_sandbox"

interface SandboxContextValue {
  isSandboxActive: boolean
  setSandboxActive: (active: boolean) => void
}

const SandboxContext = createContext<SandboxContextValue | null>(null)

export function SandboxProvider({ children }: { children: ReactNode }) {
  const [isSandboxActive, setSandboxActiveState] = useState<boolean>(() => {
    const stored = localStorage.getItem(SANDBOX_STORAGE_KEY)
    return stored === "true"
  })

  const setSandboxActive = (active: boolean) => {
    setSandboxActiveState(active)
    localStorage.setItem(SANDBOX_STORAGE_KEY, String(active))
  }

  useEffect(() => {
    const stored = localStorage.getItem(SANDBOX_STORAGE_KEY)
    if (stored !== null) {
      setSandboxActiveState(stored === "true")
    }
  }, [])

  return (
    <SandboxContext.Provider value={{ isSandboxActive, setSandboxActive }}>
      {children}
    </SandboxContext.Provider>
  )
}

export function useSandbox(): SandboxContextValue {
  const ctx = useContext(SandboxContext)
  if (!ctx) {
    throw new Error("useSandbox must be used within a SandboxProvider")
  }
  return ctx
}
