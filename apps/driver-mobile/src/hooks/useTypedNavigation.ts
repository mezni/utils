import { useNavigation } from '@react-navigation/native'
import type { NativeStackNavigationProp } from '@react-navigation/native-stack'
import type { BottomTabNavigationProp } from '@react-navigation/bottom-tabs'
import type { RootStackParamList, RootTabParamList } from '../navigation/types'

/**
 * Type-safe navigation for React Native mobile app
 * Prevents string-based navigation errors and provides type safety
 *
 * @example
 * const { toStation, toSearch, toHome } = useTypedNavigation()
 * toStation('station-123')
 * toSearch('charging station')
 */
export function useTypedNavigation() {
  const stackNav = useNavigation<NativeStackNavigationProp<RootStackParamList>>()
  const tabNav = useNavigation<BottomTabNavigationProp<RootTabParamList>>()

  return {
    toHome: () => tabNav.navigate('HomeMap'),

    toStationList: () => tabNav.navigate('StationList'),

    toSearch: (query: string) => {
      tabNav.navigate('Search', { query })
    },

    toFavorites: () => tabNav.navigate('Favorites'),

    toProfile: () => tabNav.navigate('Profile'),

    toStationDetail: (stationId: string) => {
      stackNav.navigate('StationDetail', { stationId })
    },

    toLogin: () => stackNav.navigate('LoginRegister', { mode: 'login' }),

    toRegister: () => stackNav.navigate('LoginRegister', { mode: 'register' }),

    /**
     * Navigate back
     */
    goBack: () => stackNav.goBack(),

    /**
     * Check if user can navigate back
     */
    canGoBack: () => stackNav.canGoBack(),
  }
}

/**
 * Type-safe navigation with parameters
 */
export interface TypedNavigationParams {
  toStation: (id: string) => void
  toSearch: (query: string) => void
  toHome: () => void
  toStationList: () => void
  toFavorites: () => void
  toProfile: () => void
  toStationDetail: (stationId: string) => void
  toLogin: () => void
  toRegister: () => void
  goBack: () => void
  canGoBack: () => boolean
}
