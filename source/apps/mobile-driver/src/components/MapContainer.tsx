import React, { useEffect, useRef, useState } from 'react'
import { View, StyleSheet, Platform } from 'react-native'
import MapView, {
  Marker,
  Callout,
  Region,
  PROVIDER_GOOGLE,
} from 'react-native-maps'
import * as Location from 'expo-location'
import { Station, Viewport } from '../types'
import { clampToTunisia } from '../utils/coordinates'
import { StationCallout } from './StationCallout'

interface MapContainerProps {
  stations: Station[]
  onViewportChange?: (viewport: Viewport) => void
  children?: React.ReactNode
}

export function MapContainer({ stations, onViewportChange, children }: MapContainerProps) {
  const mapRef = useRef<MapView>(null)
  const [currentRegion, setCurrentRegion] = useState<Region>({
    latitude: 36.8,
    longitude: 10.18,
    latitudeDelta: 0.1,
    longitudeDelta: 0.1,
  })

  useEffect(() => {
    async function requestLocation() {
      const { status } = await Location.requestForegroundPermissionsAsync()
      if (status !== 'granted') {
        setCurrentRegion({
          latitude: 36.8,
          longitude: 10.18,
          latitudeDelta: 0.1,
          longitudeDelta: 0.1,
        })
        return
      }

      try {
        const loc = await Location.getCurrentPositionAsync({
          accuracy: Location.Accuracy.Balanced,
        })
        const clamped = clampToTunisia(
          loc.coords.latitude,
          loc.coords.longitude,
        )
        const region: Region = {
          latitude: clamped.lat,
          longitude: clamped.lng,
          latitudeDelta: 0.05,
          longitudeDelta: 0.05,
        }
        setCurrentRegion(region)
        mapRef.current?.animateToRegion(region, 500)
      } catch {
        setCurrentRegion({
          latitude: 36.8,
          longitude: 10.18,
          latitudeDelta: 0.1,
          longitudeDelta: 0.1,
        })
      }
    }

    requestLocation()
  }, [])

  function handleRegionChangeComplete(region: Region) {
    const clamped = clampToTunisia(region.latitude, region.longitude)
    const updatedRegion: Region = {
      ...region,
      latitude: clamped.lat,
      longitude: clamped.lng,
    }

    setCurrentRegion(updatedRegion)

    if (updatedRegion.latitude !== region.latitude || updatedRegion.longitude !== region.longitude) {
      mapRef.current?.animateToRegion(updatedRegion, 0)
    }

    const zoomLevel = Math.log2(
      360 * (Platform.OS === 'ios' ? 1 : 1) / Math.max(region.longitudeDelta, 0.0001),
    )

    onViewportChange?.({
      latitude: updatedRegion.latitude,
      longitude: updatedRegion.longitude,
      latitudeDelta: updatedRegion.latitudeDelta,
      longitudeDelta: updatedRegion.longitudeDelta,
      zoomLevel: Math.round(zoomLevel * 10) / 10,
      lastUpdated: Date.now(),
    })
  }

  const provider = Platform.OS === 'android' ? PROVIDER_GOOGLE : undefined

  return (
    <View style={StyleSheet.absoluteFill}>
      <MapView
        ref={mapRef}
        style={StyleSheet.absoluteFill}
        provider={provider}
        initialRegion={currentRegion}
        onRegionChangeComplete={handleRegionChangeComplete}
        showsUserLocation
        showsMyLocationButton
        minZoomLevel={6}
        maxZoomLevel={18}
      >
        {stations.map((station) => (
          <Marker
            key={station.station_id}
            coordinate={{
              latitude: station.latitude,
              longitude: station.longitude,
            }}
            title={station.station_name}
            pinColor={station.is_private ? '#9333EA' : '#2563EB'}
          >
            <Callout>
              <StationCallout station={station} />
            </Callout>
          </Marker>
        ))}
      </MapView>
      {children}
    </View>
  )
}
