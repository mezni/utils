import { useEffect, type ReactNode } from "react"
import { useNavigate } from "react-router-dom"

export function AuthInterceptor({ children }: { children: ReactNode }) {
  const navigate = useNavigate()

  useEffect(() => {
    const originalFetch = window.fetch
    window.fetch = async (...args) => {
      const response = await originalFetch(...args)
      if (response.status === 401) {
        navigate("/login", { replace: true })
      }
      return response
    }
    return () => {
      window.fetch = originalFetch
    }
  }, [navigate])

  return <>{children}</>
}
