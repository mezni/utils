import React, { useState, useCallback, useRef } from 'react'
import { StyleSheet, View, Text, FlatList, RefreshControl, ActivityIndicator, TextInput, Alert, Platform, TouchableOpacity, Vibration } from 'react-native'
import { useStationStore } from '../store/useStationStore'
import { useThemeStore } from '../store/useThemeStore'
import { fetchStationsWithParams } from '../services/stationListService'
import { searchByAddress } from '../services/geocodingService'
import { getCachedStations, cacheStations } from '../services/offlineCache'
import { SkeletonStationItem } from '../components/SkeletonStationItem'

export default function StationListScreen() {
  const { stations, currentPage, setCurrentPage, setTotalPages, setTotalStations, setStations, setSearchQuery, setSelectedStation } = useStationStore()
  const { isDarkMode } = useThemeStore()
  const [isLoading, setIsLoading] = useState(false)
  const [isSearching, setIsSearching] = useState(false)
  const [localSearchQuery, setLocalSearchQuery] = useState('')
  const [searchResults, setSearchResults] = useState<any[]>([])
  const [isOffline, setIsOffline] = useState(false)
  const [hasCache, setHasCache] = useState(false)

  // Check offline status and cache availability
  React.useEffect(() => {
    const checkOfflineStatus = async () => {
      const cacheData = await getCachedStations()
      setHasCache(!!cacheData)
    }

    // Check network status
    const checkNetwork = async () => {
      try {
        const response = await fetch('https://www.google.com', { mode: 'no-cors' })
        setIsOffline(false)
      } catch (error) {
        setIsOffline(true)
      }
    }

    checkOfflineStatus()
    checkNetwork()

    // Periodically check network status
    const interval = setInterval(checkNetwork, 30000)
    return () => clearInterval(interval)
  }, [])

  // Haptic feedback function
  const triggerHapticFeedback = () => {
    if (Platform.OS === 'android') {
      Vibration.vibrate(50)
    }
  }

  const searchTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const debouncedSearch = useCallback((query: string) => {
    triggerHapticFeedback()
    if (searchTimeoutRef.current) {
      clearTimeout(searchTimeoutRef.current)
    }

    setIsSearching(true)

    searchTimeoutRef.current = setTimeout(async () => {
      if (query.trim().length < 2) {
        setSearchResults([])
        setSearchQuery(query)
        setCurrentPage(1)
        setIsSearching(false)
        return
      }

      try {
        const addressResults = await searchByAddress(query)

        if (addressResults.length === 0) {
          setSearchResults([])
        } else {
          setSearchResults(addressResults)
        }
      } catch (error) {
        console.error('Search failed:', error)
        Alert.alert(
          'Search Error',
          'Failed to search for stations. Please try again.',
          [{ text: 'OK' }]
        )
      } finally {
        setIsSearching(false)
      }
    }, 300)
  }, [setCurrentPage, setSearchQuery])

  const loadStations = async (page: number = 1, forceRefresh = false) => {
    setIsLoading(true)
    try {
      // Try to use cache if offline or forceRefresh is false
      if (isOffline && !forceRefresh) {
        const cacheData = await getCachedStations()
        if (cacheData) {
          console.log('Using cached stations')
          setStations(cacheData.data)
          setTotalPages(cacheData.data.length > 0 ? Math.ceil(cacheData.data.length / 20) : 1)
          setTotalStations(cacheData.data.length)
          return
        }
      }

      const response = await fetchStationsWithParams({ page, per_page: 20 })

      // Cache the stations for offline use
      await cacheStations(response.data)

      setStations(response.data)
      setTotalPages(response.meta.total_pages)
      setTotalStations(response.meta.total)
    } catch (error) {
      console.error('Failed to fetch stations:', error)
    } finally {
      setIsLoading(false)
    }
  }

  const loadMore = () => {
    if (currentPage < useStationStore.getState().totalPages && !isLoading) {
      const nextPage = currentPage + 1
      setCurrentPage(nextPage)
      loadStations(nextPage)
    }
  }

  const handleRefresh = async () => {
    triggerHapticFeedback()
    setCurrentPage(1)
    await loadStations(1, true)
  }

  const handleSearch = (query: string) => {
    triggerHapticFeedback()
    debouncedSearch(query)
  }

  const handleRetry = async () => {
    await loadStations(1, true)
  }

  const renderItem = ({ item }: { item: any }) => {
    return (
      <View style={[styles.stationItem, { backgroundColor: isDarkMode ? '#1a1a1a' : '#ffffff' }]}>
        <View style={styles.stationHeader}>
          <Text style={[styles.stationName, { color: isDarkMode ? '#fff' : '#000' }]}>
            {item.name}
          </Text>
          {item.distance_km !== undefined && (
            <Text style={[styles.stationDistance, { color: isDarkMode ? '#4ade80' : '#10B981' }]}>
              {item.distance_km?.toFixed(1)} km away
            </Text>
          )}
        </View>
        <Text style={[styles.stationAddress, { color: isDarkMode ? '#999' : '#666' }]}>
          {item.address}
        </Text>
        <View style={styles.amenitiesContainer}>
          {item.amenities?.map((amenity: string, index: number) => (
            <View
              key={index}
              style={[styles.amenityBadge, { backgroundColor: isDarkMode ? '#333' : '#f0f0f0' }]}
            >
              <Text style={[styles.amenityText, { color: isDarkMode ? '#ccc' : '#666' }]}>
                {amenity}
              </Text>
            </View>
          ))}
        </View>
      </View>
    )
  }

  const handleStationPress = (station: any) => {
    triggerHapticFeedback()
    setSelectedStation(station)
    // TODO: Navigate to station detail
    // navigation.navigate('station/[id]', { id: station.id })
  }

  const renderFooter = () => {
    if (isLoading && stations.length > 0) {
      return (
        <View style={styles.footerContainer}>
          <ActivityIndicator size="small" color="#2563eb" />
          <Text style={[styles.footerText, { color: isDarkMode ? '#999' : '#666' }]}>
            Loading more stations...
          </Text>
        </View>
      )
    }
    return null
  }

  const renderOfflineBanner = () => {
    if (!isOffline) return null

    return (
      <View style={[styles.offlineBanner, { backgroundColor: isDarkMode ? '#444' : '#f3f4f6' }]}>
        <Text style={[styles.offlineBannerText, { color: isDarkMode ? '#fff' : '#000' }]}>
          ⚠️ You are offline
        </Text>
        <Text style={[styles.offlineBannerSubtext, { color: isDarkMode ? '#999' : '#666' }]}>
          Showing cached data
        </Text>
        <TouchableOpacity
          style={[styles.retryButton, { backgroundColor: isDarkMode ? '#2563eb' : '#2563eb' }]}
          onPress={handleRetry}
        >
          <Text style={[styles.retryButtonText, { color: '#fff' }]}>Retry</Text>
        </TouchableOpacity>
      </View>
    )
  }

  const renderCacheIndicator = () => {
    if (!hasCache) return null

    return (
      <View style={[styles.cacheIndicator, { backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5' }]}>
        <Text style={[styles.cacheIndicatorText, { color: isDarkMode ? '#4ade80' : '#10B981' }]}>
          📦 Cached data available
        </Text>
      </View>
    )
  }

  return (
    <View style={[styles.container, { backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5' }]}>
      {renderCacheIndicator()}
      {renderOfflineBanner()}

      <View style={styles.searchContainer}>
        <TextInput
          style={[styles.searchInput, {
            backgroundColor: isDarkMode ? '#2a2a2a' : '#ffffff',
            color: isDarkMode ? '#fff' : '#000',
          }]}
          placeholder="Search stations..."
          placeholderTextColor={isDarkMode ? '#666' : '#999'}
          value={localSearchQuery}
          onChangeText={handleSearch}
          clearButtonMode="while-editing"
        />
        {isSearching && (
          <ActivityIndicator
            style={styles.searchIndicator}
            size="small"
            color="#2563eb"
          />
        )}
      </View>

      {searchResults.length > 0 && (
        <View style={styles.resultsHeader}>
          <Text style={[styles.resultsTitle, { color: isDarkMode ? '#fff' : '#000' }]}>
            Search Results ({searchResults.length})
          </Text>
          <Text style={[styles.resultsSubtitle, { color: isDarkMode ? '#999' : '#666' }]}>
            {searchResults[0].display_name}
          </Text>
        </View>
      )}

      <FlatList
        data={stations}
        keyExtractor={(item) => item.id}
        renderItem={renderItem}
        contentContainerStyle={styles.listContent}
        refreshControl={
          <RefreshControl refreshing={isLoading} onRefresh={handleRefresh} />
        }
        onEndReached={loadMore}
        onEndReachedThreshold={0.5}
        ListEmptyComponent={
          isLoading ? (
            <>
              {[1, 2, 3, 4, 5].map((i) => (
                <SkeletonStationItem key={i} />
              ))}
            </>
          ) : stations.length === 0 && searchResults.length === 0 ? (
            <View style={styles.emptyContainer}>
              <Text style={[styles.emptyText, { color: isDarkMode ? '#999' : '#666' }]}>
                No stations found
              </Text>
              <Text style={[styles.emptySubtext, { color: isDarkMode ? '#666' : '#999' }]}>
                Try adjusting your search or filters
              </Text>
            </View>
          ) : null
        }
        ListFooterComponent={renderFooter}
      />
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  cacheIndicator: {
    padding: 12,
    borderBottomWidth: 1,
  },
  cacheIndicatorText: {
    fontSize: 14,
    fontWeight: 'bold',
  },
  offlineBanner: {
    padding: 12,
    borderBottomWidth: 1,
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    gap: 8,
  },
  offlineBannerText: {
    fontSize: 14,
    fontWeight: 'bold',
  },
  offlineBannerSubtext: {
    fontSize: 12,
  },
  retryButton: {
    paddingHorizontal: 16,
    paddingVertical: 8,
    borderRadius: 8,
  },
  retryButtonText: {
    fontSize: 14,
    fontWeight: 'bold',
  },
  searchContainer: {
    flexDirection: 'row',
    padding: 12,
    borderBottomWidth: 1,
  },
  searchInput: {
    flex: 1,
    padding: 12,
    borderRadius: 8,
    fontSize: 16,
  },
  searchIndicator: {
    marginLeft: 8,
  },
  resultsHeader: {
    padding: 16,
    borderBottomWidth: 1,
  },
  resultsTitle: {
    fontSize: 18,
    fontWeight: 'bold',
    marginBottom: 4,
  },
  resultsSubtitle: {
    fontSize: 14,
  },
  listContent: {
    padding: 16,
  },
  stationItem: {
    padding: 16,
    borderRadius: 8,
    marginBottom: 12,
    elevation: 2,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.1,
    shadowRadius: 4,
  },
  stationHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: 8,
  },
  stationName: {
    fontSize: 16,
    fontWeight: 'bold',
    flex: 1,
  },
  stationDistance: {
    fontSize: 12,
    marginLeft: 8,
  },
  stationAddress: {
    fontSize: 14,
    marginBottom: 12,
  },
  amenitiesContainer: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 6,
  },
  amenityBadge: {
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 4,
  },
  amenityText: {
    fontSize: 12,
  },
  emptyContainer: {
    flex: 1,
    justifyContent: 'center',
    alignItems: 'center',
    marginTop: 50,
    paddingHorizontal: 32,
    textAlign: 'center',
  },
  emptyText: {
    fontSize: 16,
  },
  emptySubtext: {
    fontSize: 14,
    marginTop: 8,
  },
  footerContainer: {
    flexDirection: 'row',
    justifyContent: 'center',
    alignItems: 'center',
    padding: 20,
    gap: 10,
  },
  footerText: {
    fontSize: 12,
  },
})