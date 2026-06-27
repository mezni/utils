import { createBrowserRouter, RouterProvider } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { useEffect } from 'react'
import { useAuthStore } from '@/stores/auth-store'
import { AppLayout } from '@/components/layout/AppLayout'
import { GuestGuard } from '@/components/guards/AuthGuard'
import { RouteGuard, AdminRoute, PublicRoute, UnauthorizedPage } from '@/components/guards/RouteGuard'
import { LoginPage } from '@/features/auth/LoginPage'
import { DashboardPage } from '@/features/dashboard/DashboardPage'
import { UsersPage } from '@/features/users/UsersPage'

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
      staleTime: 30_000,
      refetchOnWindowFocus: true,
    },
  },
})

function HydrateAuth({ children }: { children: React.ReactNode }) {
  const hydrate = useAuthStore((s) => s.hydrate)
  useEffect(() => { hydrate() }, [hydrate])
  return <>{children}</>
}

const router = createBrowserRouter([
  {
    path: '/login',
    element: (
      <PublicRoute>
        <LoginPage />
      </PublicRoute>
    ),
  },
  {
    path: '/unauthorized',
    element: <UnauthorizedPage />,
  },
  {
    path: '/',
    element: (
      <RouteGuard>
        <AppLayout />
      </RouteGuard>
    ),
    children: [
      { index: true, element: <AdminRoute><DashboardPage /></AdminRoute> },
      { path: 'users', element: <AdminRoute><UsersPage /></AdminRoute> },
      // Add more routes with role protection as needed
    ],
  },
])

export function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <HydrateAuth>
        <RouterProvider router={router} />
      </HydrateAuth>
    </QueryClientProvider>
  )
}
