import React, { useState } from 'react'
import { View, StyleSheet, Text, TouchableOpacity, ScrollView } from 'react-native'
import { useThemeStore } from '../store/useThemeStore'
import { useMapStore } from '../store/useMapStore'
import Animated, {
  FadeIn,
  FadeOut,
  SlideInUp,
  SlideOutDown,
} from 'react-native-reanimated'

interface Station {
  id: string
  name: string
  address: string
  distance_km: number
  chargers?: Array<{
    type: string
    status: string
    power: number
  }>
}

interface StationPreviewBottomSheetProps {
  station?: Station
  isVisible: boolean
  onClose: () => void
  onNavigate: () => void
}

export default function StationPreviewBottomSheet({
  station,
  isVisible,
  onClose,
  onNavigate,
}: StationPreviewBottomSheetProps) {
  const { isDarkMode } = useThemeStore()
  const { setSelectedStation } = useMapStore()

  if (!isVisible || !station) {
    return null
  }

  return (
    <Animated.View
      entering={SlideInUp.duration(300)}
      exiting={SlideOutDown.duration(300)}
      style={[
        styles.container,
        { backgroundColor: isDarkMode ? '#2a2a2a' : '#fff' },
      ]}
    >
      <View style={styles.handle} />
      <View style={styles.content}>
        <View style={styles.header}>
          <View style={styles.stationNameContainer}>
            <Text style={[styles.stationName, { color: isDarkMode ? '#fff' : '#000' }]}>
              {station.name}
            </Text>
            <Text style={[styles.stationAddress, { color: isDarkMode ? '#999' : '#666' }]}>
              {station.address}
            </Text>
          </View>
          <TouchableOpacity onPress={onClose} style={styles.closeButton}>
            <Text style={[styles.closeButtonText, { color: isDarkMode ? '#fff' : '#000' }]}>
              ✕
            </Text>
          </TouchableOpacity>
        </View>

        <View style={styles.infoSection}>
          <Text style={[styles.sectionTitle, { color: isDarkMode ? '#fff' : '#000' }]}>
            Station Details
          </Text>
          <View style={styles.detailRow}>
            <Text style={[styles.detailLabel, { color: isDarkMode ? '#999' : '#666' }]}>
              Distance
            </Text>
            <Text style={[styles.detailValue, { color: isDarkMode ? '#4ade80' : '#16a34a' }]}>
              {station.distance_km.toFixed(1)} km
            </Text>
          </View>
          <View style={styles.detailRow}>
            <Text style={[styles.detailLabel, { color: isDarkMode ? '#999' : '#666' }]}>
              Type
            </Text>
            <Text style={[styles.detailValue, { color: isDarkMode ? '#fff' : '#000' }]}>
              Charging Station
            </Text>
          </View>
          <View style={styles.detailRow}>
            <Text style={[styles.detailLabel, { color: isDarkMode ? '#999' : '#666' }]}>
              Amenities
            </Text>
            <Text style={[styles.detailValue, { color: isDarkMode ? '#fff' : '#000' }]}>
              Parking, WiFi
            </Text>
          </View>
        </View>

        <View style={styles.chargersSection}>
          <Text style={[styles.sectionTitle, { color: isDarkMode ? '#fff' : '#000' }]}>
            Available Chargers
          </Text>
          {station.chargers && station.chargers.length > 0 ? (
            <View style={styles.chargerList}>
              {station.chargers.map((charger, index) => (
                <View
                  key={index}
                  style={[
                    styles.chargerItem,
                    { backgroundColor: isDarkMode ? '#333' : '#f5f5f5' },
                  ]}
                >
                  <View style={styles.chargerInfo}>
                    <Text style={[styles.chargerType, { color: isDarkMode ? '#fff' : '#000' }]}>
                      {charger.type}
                    </Text>
                    <Text style={[styles.chargerPower, { color: isDarkMode ? '#999' : '#666' }]}>
                      {charger.power} kW
                    </Text>
                  </View>
                  <View
                    style={[
                      styles.statusIndicator,
                      {
                        backgroundColor:
                          charger.status === 'available' ? '#16a34a' : '#dc2626',
                      },
                    ]}
                  >
                    <Text style={styles.statusText}>
                      {charger.status}
                    </Text>
                  </View>
                </View>
              ))}
            </View>
          ) : (
            <Text style={[styles.noChargers, { color: isDarkMode ? '#999' : '#666' }]}>
              No chargers available at this station
            </Text>
          )}
        </View>

        <TouchableOpacity
          style={[
            styles.navigateButton,
            { backgroundColor: isDarkMode ? '#2563eb' : '#2563eb' },
          ]}
          onPress={() => {
            setSelectedStation(station as any)
            onNavigate()
          }}
        >
          <Text style={styles.navigateButtonText}>Navigate to Station</Text>
        </TouchableOpacity>
      </View>
    </Animated.View>
  )
}

const styles = StyleSheet.create({
  container: {
    position: 'absolute',
    bottom: 0,
    left: 0,
    right: 0,
    borderTopLeftRadius: 20,
    borderTopRightRadius: 20,
    maxHeight: '80%',
    minHeight: 400,
    shadowColor: '#000',
    shadowOffset: { width: 0, height: -2 },
    shadowOpacity: 0.3,
    shadowRadius: 8,
    elevation: 10,
    overflow: 'hidden',
  },
  handle: {
    width: 40,
    height: 4,
    backgroundColor: '#ccc',
    borderRadius: 2,
    alignSelf: 'center',
    marginTop: 12,
  },
  content: {
    padding: 20,
    paddingBottom: 30,
  },
  header: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'flex-start',
    marginBottom: 20,
  },
  stationNameContainer: {
    flex: 1,
    marginRight: 10,
  },
  stationName: {
    fontSize: 20,
    fontWeight: 'bold',
  },
  stationAddress: {
    fontSize: 14,
    marginTop: 4,
  },
  closeButton: {
    padding: 8,
    borderRadius: 8,
  },
  closeButtonText: {
    fontSize: 20,
    fontWeight: 'bold',
  },
  infoSection: {
    marginBottom: 20,
  },
  sectionTitle: {
    fontSize: 16,
    fontWeight: '600',
    marginBottom: 12,
  },
  detailRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    marginBottom: 8,
  },
  detailLabel: {
    fontSize: 14,
  },
  detailValue: {
    fontSize: 14,
    fontWeight: '600',
  },
  chargersSection: {
    marginBottom: 20,
  },
  chargerList: {
    gap: 8,
  },
  chargerItem: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: 12,
    borderRadius: 8,
  },
  chargerInfo: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
  },
  chargerType: {
    fontSize: 14,
    fontWeight: '600',
  },
  chargerPower: {
    fontSize: 12,
  },
  statusIndicator: {
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 4,
    minWidth: 80,
    alignItems: 'center',
  },
  statusText: {
    color: '#fff',
    fontSize: 12,
    fontWeight: '600',
  },
  noChargers: {
    fontSize: 14,
    fontStyle: 'italic',
  },
  navigateButton: {
    padding: 16,
    borderRadius: 12,
    alignItems: 'center',
    shadowColor: '#2563eb',
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.3,
    shadowRadius: 4,
    elevation: 2,
  },
  navigateButtonText: {
    color: '#fff',
    fontSize: 16,
    fontWeight: 'bold',
  },
})
