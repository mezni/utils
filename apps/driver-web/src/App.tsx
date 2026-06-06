import { createBrowserRouter } from 'react-router-dom'

export const router = createBrowserRouter([
  {
    path: '/',
    lazy: () => import('./screens/HomeMapScreen').then(m => ({ Component: m.default })),
  },
  {
    path: '/stations/:id',
    lazy: () => import('./screens/StationDetailScreen').then(m => ({ Component: m.default })),
  },
  {
    path: '/search',
    lazy: () => import('./screens/SearchResultsScreen').then(m => ({ Component: m.default })),
  },
  {
    path: '/favorites',
    lazy: () => import('./screens/FavoritesScreen').then(m => ({ Component: m.default })),
  },
  {
    path: '/profile',
    lazy: () => import('./screens/ProfileScreen').then(m => ({ Component: m.default })),
  },
  {
    path: '/login',
    lazy: () => import('./screens/LoginRegisterScreen').then(m => ({ Component: m.default })),
  },
])
