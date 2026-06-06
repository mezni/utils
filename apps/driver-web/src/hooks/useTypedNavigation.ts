import { useNavigate, useSearchParams } from 'react-router-dom'

/**
 * Type-safe navigation routes
 */
export enum NavigationRoute {
  Home = '/',
  StationDetail = '/stations/:id',
  Search = '/search',
  Favorites = '/favorites',
  Profile = '/profile',
  Login = '/login',
}

/**
 * Navigation parameters for each route
 */
export interface NavigationParams {
  [NavigationRoute.Home]: Record<string, never>
  [NavigationRoute.StationDetail]: { id: string }
  [NavigationRoute.Search]: { q?: string }
  [NavigationRoute.Favorites]: Record<string, never>
  [NavigationRoute.Profile]: Record<string, never>
  [NavigationRoute.Login]: Record<string, never>
}

/**
 * Type-safe navigation hook for React Router
 * Prevents string-based navigation errors and provides type safety
 *
 * @example
 * const { toStation, toSearch, toHome } = useTypedNavigation()
 * toStation('station-123')
 * toSearch('charging station')
 */
export function useTypedNavigation() {
  const navigate = useNavigate()
  const [searchParams] = useSearchParams()

  return {
    toHome: () => navigate(NavigationRoute.Home),

    toStation: (id: string) => {
      navigate(NavigationRoute.StationDetail.replace(':id', id))
    },

    toSearch: (query: string) => {
      navigate(`${NavigationRoute.Search}?q=${encodeURIComponent(query)}`)
    },

    toFavorites: () => navigate(NavigationRoute.Favorites),

    toProfile: () => navigate(NavigationRoute.Profile),

    toLogin: () => navigate(NavigationRoute.Login),

    /**
     * Get current search query from URL
     */
    getSearchQuery: () => searchParams.get('q') || '',

    /**
     * Navigate back
     */
    goBack: () => navigate(-1),

    /**
     * Navigate with full path
     */
    navigateTo: (path: string, replace = false) => navigate(path, { replace }),
  }
}

/**
 * Custom hook to validate route parameters
 */
export function useRouteParams<T extends NavigationRoute>(
  route: T,
): NavigationParams[T] {
  const [searchParams] = useSearchParams()

  if (route === NavigationRoute.Search) {
    return { q: searchParams.get('q') || '' } as NavigationParams[T]
  }

  return {} as NavigationParams[T]
}
