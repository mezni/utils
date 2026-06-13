import { useState, useEffect } from 'react'
import { View, StyleSheet, Text, ActivityIndicator, Alert } from 'react-native'
import MapView, { Marker, Callout, PROVIDER_GOOGLE } from 'react-native-maps'
import { useThemeStore } from '../store/useThemeStore'
import { useMapStore } from '../store/useMapStore'
import { useNearbyStations } from '../services/queryClient'
import { requestGeolocationPermission } from '../services/geolocation'
import Animated, { SlideInUp } from 'react-native-reanimated'
import { clusterMarkers } from '../utils/mapCluster'

const INITIAL_REGION = {
  latitude: 36.8065,
  longitude: 10.1815,
  latitudeDelta: 0.0922,
  longitudeDelta: 0.0421,
}

export default function MapScreen() {
  const { isDarkMode } = useThemeStore()
  const { center, markers, setUserLocation, setCenter, setZoom, addMarkers } = useMapStore()
  const [loading, setLoading] = useState(true)
  const [selectedStation, setSelectedStation] = useState<any>(null)

  const { data: nearbyStations } = useNearbyStations(
    center.lat,
    center.lng,
    center.lat !== 36.8065 ? 10 : 10
  )

  useEffect(() => {
    requestGeolocationPermission()
  }, [])

  useEffect(() => {
    if (nearbyStations?.data && nearbyStations?.data.length > 0) {
      const stationMarkers = nearbyStations.data.map((station: any) => ({
        key: station.id,
        coordinate: {
          latitude: station.geometry?.coordinates[1] || 36.8065,
          longitude: station.geometry?.coordinates[0] || 10.1815,
        },
        title: station.name,
        description: station.address,
      }))
      addMarkers(stationMarkers)
      setLoading(false)
    } else if (nearbyStations?.data && nearbyStations?.data.length === 0) {
      // No stations found but query succeeded
      setLoading(false)
    }
  }, [nearbyStations, addMarkers])

  const handleRefresh = () => {
    setLoading(true)
    setTimeout(() => setLoading(false), 1000)
  }

  const handleMarkerPress = (marker: any) => {
    setSelectedStation(marker)
    // Simulate haptic feedback
    Alert.alert('Station Selected', `${marker.name} at ${marker.address}`)
  }

  const handleRegionChange = (region: any) => {
    const newCenter = {
      lat: region.latitude,
      lng: region.longitude,
    }
    setCenter(newCenter)
    setZoom(region.zoomLevel)
  }

  if (loading) {
    return (
      <View style={[styles.container, { backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5' }]}>
        <ActivityIndicator size="large" color="#2563eb" />
      </View>
    )
  }

  const clusteredMarkers = clusterMarkers(markers, 50)

  return (
    <View style={[styles.container, { backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5' }]}>
      <MapView
        style={styles.map}
        provider={PROVIDER_GOOGLE}
        initialRegion={INITIAL_REGION}
        region={{
          latitude: center.lat,
          longitude: center.lng,
          latitudeDelta: 0.1,
          longitudeDelta: 0.1,
        }}
        onRegionChangeComplete={handleRegionChange}
        showsUserLocation={true}
        showsMyLocationButton={true}
      >
        {clusteredMarkers.map((cluster) => (
          <Marker
            key={cluster.key}
            coordinate={cluster.coordinate}
            title={cluster.stationCount > 1 ? `${cluster.stationCount} Stations` : cluster.markers[0].title}
            description={cluster.stationCount > 1 ? 'Cluster of stations' : cluster.markers[0].description}
            onPress={() => {
              if (cluster.stationCount > 1) {
                Alert.alert(
                  `${cluster.stationCount} Stations`,
                  'Click on a station from the list below'
                )
              } else {
                handleMarkerPress(cluster.markers[0])
              }
            }}
          >
            <Callout>
              <View style={[styles.callout, { backgroundColor: isDarkMode ? '#333' : '#fff' }]}>
                <Text style={[styles.calloutTitle, { color: isDarkMode ? '#fff' : '#000' }]}>
                  {cluster.stationCount > 1
                    ? `${cluster.stationCount} Stations`
                    : cluster.markers[0].title}
                </Text>
                <Text style={[styles.calloutAddress, { color: isDarkMode ? '#999' : '#666' }]}>
                  {cluster.stationCount > 1
                    ? 'Cluster of stations nearby'
                    : cluster.markers[0].description}
                </Text>
                <Text style={[styles.calloutDistance, { color: isDarkMode ? '#4ade80' : '#16a34a' }]}>
                  {cluster.stationCount > 1 ? 'Multiple stations' : '1 station'}
                </Text>
              </View>
            </Callout>
          </Marker>
        ))}
      </MapView>

      {selectedStation && (
        <Animated.View entering={SlideInUp.duration(300)}>
          <View style={[styles.stationPreview, { backgroundColor: isDarkMode ? '#2a2a2a' : '#fff' }]}>
            <Text style={[styles.stationPreviewTitle, { color: isDarkMode ? '#fff' : '#000' }]}>
              {selectedStation.name}
            </Text>
            <Text style={[styles.stationPreviewAddress, { color: isDarkMode ? '#999' : '#666' }]}>
              {selectedStation.address}
            </Text>
            <Text style={[styles.stationPreviewDistance, { color: isDarkMode ? '#4ade80' : '#16a34a' }]}>
              {selectedStation.distance_km?.toFixed(1)} km away
            </Text>
            <Text style={[styles.stationPreviewClose, { color: isDarkMode ? '#fff' : '#000' }]}>
              Close Station
            </Text>
          </View>
        </Animated.View>
      )}
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  map: {
    flex: 1,
  },
  callout: {
    padding: 10,
    borderRadius: 8,
    minWidth: 150,
  },
  calloutTitle: {
    fontSize: 14,
    fontWeight: '600',
  },
  calloutAddress: {
    fontSize: 12,
    marginTop: 4,
  },
  calloutDistance: {
    fontSize: 12,
    marginTop: 4,
    fontWeight: '600',
  },
  stationPreview: {
    position: 'absolute',
    bottom: 20,
    left: 20,
    right: 20,
    padding: 16,
    borderRadius: 12,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.3,
    shadowRadius: 4,
    elevation: 5,
  },
  stationPreviewTitle: {
    fontSize: 16,
    fontWeight: 'bold',
  },
  stationPreviewAddress: {
    fontSize: 14,
    marginTop: 4,
  },
  stationPreviewDistance: {
    fontSize: 12,
    marginTop: 4,
    fontWeight: '600',
  },
  stationPreviewClose: {
    fontSize: 14,
    fontWeight: 'bold',
    marginTop: 8,
    paddingVertical: 8,
    paddingHorizontal: 16,
    backgroundColor: '#2563eb',
    borderRadius: 8,
    textAlign: 'center',
  },
})