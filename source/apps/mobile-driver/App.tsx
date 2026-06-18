import { useCallback, useMemo, useState } from 'react'
import { View, StyleSheet, RefreshControl, LayoutAnimation, Platform, UIManager } from 'react-native'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { MapContainer } from './src/components/MapContainer'
import { useNearbyStations } from './src/hooks/useNearbyStations'
import { useDebounce } from './src/hooks/useDebounce'
import { ShimmerSkeleton } from './src/components/ShimmerSkeleton'
import { ErrorBoundary } from './src/components/ErrorBoundary'
import { EmptyState } from './src/components/EmptyState'
import { OfflineBanner } from './src/components/OfflineBanner'
import { MacroZoomOverlay } from './src/components/MacroZoomOverlay'
import { Viewport, ApiFetchState } from './src/types'

if (
  Platform.OS === 'android' &&
  UIManager.setLayoutAnimationEnabledExperimental
) {
  UIManager.setLayoutAnimationEnabledExperimental(true)
}

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 3,
      staleTime: 60000,
    },
  },
})

function AppContent() {
  const [viewport, setViewport] = useState<Viewport | null>(null)

  const rawLat = viewport?.latitude ?? 36.8
  const rawLng = viewport?.longitude ?? 10.18

  const debouncedLat = useDebounce(rawLat, 300)
  const debouncedLng = useDebounce(rawLng, 300)

  const {
    stations,
    isLoading,
    isError,
    error,
    refetch,
    isFetching,
    isOffline,
    cachedStations,
  } = useNearbyStations(debouncedLat, debouncedLng)

  const fetchState: ApiFetchState = useMemo(() => {
    if (isOffline && cachedStations) {
      return { type: 'offline', stations: cachedStations }
    }
    if (isLoading) return { type: 'loading' }
    if (isError) return { type: 'error', message: error?.message ?? 'Unknown error' }
    if (stations.length === 0) return { type: 'empty' }
    return { type: 'success', stations }
  }, [isOffline, cachedStations, isLoading, isError, error, stations])

  const handleRefresh = useCallback(() => {
    LayoutAnimation.configureNext(LayoutAnimation.Presets.easeInEaseOut)
    refetch()
  }, [refetch])

  function handleViewportChange(newViewport: Viewport) {
    setViewport(newViewport)
  }

  const showMacroZoom = viewport !== null && viewport.zoomLevel < 8

  return (
    <View style={styles.container}>
      {fetchState.type === 'offline' && <OfflineBanner />}

      <MapContainer
        stations={
          fetchState.type === 'success' || fetchState.type === 'offline'
            ? fetchState.stations
            : []
        }
        onViewportChange={handleViewportChange}
      >
        {fetchState.type === 'loading' && <ShimmerSkeleton />}

        {fetchState.type === 'empty' && <EmptyState />}

        {showMacroZoom && <MacroZoomOverlay />}

        <RefreshControl
          refreshing={isFetching && !isLoading}
          onRefresh={handleRefresh}
          tintColor="#2563EB"
        />
      </MapContainer>

      {fetchState.type === 'error' && (
        <ErrorBoundary
          message={fetchState.message}
          onRetry={handleRefresh}
        />
      )}
    </View>
  )
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <AppContent />
    </QueryClientProvider>
  )
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
})
