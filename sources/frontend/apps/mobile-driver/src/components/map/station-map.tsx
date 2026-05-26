import { useState, useEffect, useRef, useCallback } from "react"
import { View, Text, StyleSheet, ActivityIndicator, TouchableOpacity } from "react-native"
import MapView, { Marker, Region } from "react-native-maps"
import { Station } from "../../types/station"
import { fetchNearbyStations } from "../../services/nearby-api"
import { requestLocation } from "../../services/location"

const TUNISIA_CENTER = { latitude: 33.8869, longitude: 9.5375 }

interface StationMapProps {
  radius?: number
  onStationSelect?: (station: Station) => void
}

export function StationMap({ radius = 20000, onStationSelect }: StationMapProps) {
  const mapRef = useRef<MapView>(null)
  const [stations, setStations] = useState<Station[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [locationDenied, setLocationDenied] = useState(false)
  const [currentRegion, setCurrentRegion] = useState<Region>({
    ...TUNISIA_CENTER,
    latitudeDelta: 10,
    longitudeDelta: 10,
  })

  const loadStations = useCallback(async (lat: number, lng: number, rad: number) => {
    setLoading(true)
    setError(null)
    try {
      const data = await fetchNearbyStations(lat, lng, rad)
      setStations(data)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load stations")
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    requestLocation().then((result) => {
      if (result.permissionGranted) {
        setCurrentRegion({
          latitude: result.latitude,
          longitude: result.longitude,
          latitudeDelta: 0.1,
          longitudeDelta: 0.1,
        })
        mapRef.current?.animateToRegion({
          latitude: result.latitude,
          longitude: result.longitude,
          latitudeDelta: 0.1,
          longitudeDelta: 0.1,
        })
        loadStations(result.latitude, result.longitude, radius)
      } else {
        setLocationDenied(true)
        setLoading(false)
      }
    })
  }, [radius, loadStations])

  return (
    <View style={StyleSheet.absoluteFillObject}>
      <MapView
        ref={mapRef}
        style={StyleSheet.absoluteFillObject}
        initialRegion={currentRegion}
        showsUserLocation
        showsMyLocationButton
        onRegionChangeComplete={(region) => setCurrentRegion(region)}
      >
        {stations.map((station) => (
          <Marker
            key={station.id}
            coordinate={{ latitude: station.latitude, longitude: station.longitude }}
            title={station.name}
            description={`${station.city} · ${station.distance_meters.toFixed(0)}m`}
            onPress={() => onStationSelect?.(station)}
          />
        ))}
      </MapView>

      {loading && (
        <View style={styles.overlay}>
          <ActivityIndicator size="large" color="#22c55e" />
          <Text style={styles.overlayText}>Finding nearby stations...</Text>
        </View>
      )}

      {error && (
        <View style={styles.overlay}>
          <Text style={styles.errorText}>{error}</Text>
          <TouchableOpacity
            onPress={() => {
              loadStations(currentRegion.latitude, currentRegion.longitude, radius)
            }}
          >
            <Text style={styles.retryText}>Tap to retry</Text>
          </TouchableOpacity>
        </View>
      )}

      {locationDenied && (
        <View style={styles.overlay}>
          <Text style={styles.overlayText}>
            Location access is needed to find nearby stations. Enable it in Settings.
          </Text>
        </View>
      )}

      {!loading && !error && !locationDenied && stations.length === 0 && (
        <View style={styles.overlay}>
          <Text style={styles.overlayText}>No stations found nearby.</Text>
        </View>
      )}
    </View>
  )
}

const styles = StyleSheet.create({
  overlay: {
    position: "absolute",
    bottom: 100,
    left: 20,
    right: 20,
    backgroundColor: "rgba(255,255,255,0.95)",
    borderRadius: 16,
    padding: 20,
    alignItems: "center",
    shadowColor: "#000",
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.1,
    shadowRadius: 8,
    elevation: 4,
  },
  overlayText: {
    fontSize: 14,
    color: "#666",
    textAlign: "center",
  },
  errorText: {
    fontSize: 14,
    color: "#dc2626",
    textAlign: "center",
  },
  retryText: {
    fontSize: 14,
    color: "#22c55e",
    fontWeight: "600",
    marginTop: 8,
  },
})
