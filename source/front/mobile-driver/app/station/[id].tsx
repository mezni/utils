import React, { useState } from 'react'
import { StyleSheet, View, Text, ScrollView, Image, TouchableOpacity } from 'react-native'
import { useThemeStore } from '../../store/useThemeStore'
import { useStationStore } from '../../store/useStationStore'
import { useQuery } from '@tanstack/react-query'
import { fetchStationDetailById } from '../../services/stationDetailService'
import { SkeletonStationDetail } from '../../components/SkeletonStationDetail'

export default function StationDetailScreen() {
  const { selectedStation, setSelectedStation } = useStationStore()
  const { isDarkMode } = useThemeStore()
  const [showImages, setShowImages] = useState(false)

  const { data: station, isLoading, isError } = useQuery({
    queryKey: ['station', selectedStation?.id],
    queryFn: () => fetchStationDetailById(selectedStation?.id || ''),
    enabled: !!selectedStation?.id,
    staleTime: 600000, // 10 minutes
  })

  const handleBack = () => {
    setSelectedStation(null)
    // navigation.goBack()
  }

  const handleNavigation = () => {
    // TODO: Navigate to external mapping app
    console.log('Navigate to:', selectedStation)
  }

  const handleDirections = () => {
    // TODO: Open directions in external map app
    console.log('Open directions for:', selectedStation)
  }

  if (!selectedStation) {
    return (
      <View style={[styles.container, { backgroundColor: isDarkMode ? '#1a1a1a' : '#ffffff' }]}>
        <Text style={[styles.backButton, { color: isDarkMode ? '#fff' : '#000' }]}>Back</Text>
        <Text style={[styles.message, { color: isDarkMode ? '#999' : '#666' }]}>No station selected</Text>
      </View>
    )
  }

  if (isLoading) {
    return (
      <ScrollView style={[styles.container, { backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5' }]}>
        <SkeletonStationDetail />
        <SkeletonStationDetail />
        <SkeletonStationDetail />
      </ScrollView>
    )
  }

  if (isError || !station) {
    return (
      <View style={[styles.container, { backgroundColor: isDarkMode ? '#1a1a1a' : '#ffffff' }]}>
        <Text style={[styles.backButton, { color: isDarkMode ? '#fff' : '#000' }]}>Back</Text>
        <Text style={[styles.message, { color: isDarkMode ? '#ef4444' : '#ef4444' }]}>
          Failed to load station
        </Text>
      </View>
    )
  }

  const getChargeRate = (charger: any) => {
    // Simulate charge rate based on charger type and power
    if (charger.charger_type === 'CCS' && charger.power_kw >= 50) {
      return '€0.30/kWh'
    } else if (charger.charger_type === 'CHAdeMO') {
      return '€0.35/kWh'
    } else {
      return '€0.25/kWh'
    }
  }

  return (
    <ScrollView style={[styles.container, { backgroundColor: isDarkMode ? '#1a1a1a' : '#f5f5f5' }]}>
      <View style={styles.header}>
        <Text style={[styles.backButton, { color: isDarkMode ? '#fff' : '#000' }]} onPress={handleBack}>
          ← Back
        </Text>
        <Text style={[styles.title, { color: isDarkMode ? '#fff' : '#000' }]}>
          {station.name}
        </Text>
      </View>

      {/* Station Images Section */}
      {station.images && station.images.length > 0 && (
        <View style={styles.imagesSection}>
          <TouchableOpacity
            style={[styles.imagesHeader, { backgroundColor: isDarkMode ? '#2a2a2a' : '#f5f5f5' }]}
            onPress={() => setShowImages(!showImages)}
          >
            <Text style={[styles.imagesTitle, { color: isDarkMode ? '#fff' : '#000' }]}>
              Photos ({station.images.length})
            </Text>
            <Text style={[styles.imagesChevron, { color: isDarkMode ? '#fff' : '#000' }]}>
              {showImages ? '▼' : '▶'}
            </Text>
          </TouchableOpacity>

          {showImages && (
            <View style={styles.imagesGrid}>
              {station.images.map((image: any, index: number) => (
                <Image
                  key={index}
                  source={{ uri: image.url }}
                  style={[
                    styles.imageItem,
                    index % 3 === 2 ? styles.imageItemLast : styles.imageItemSingle,
                  ]}
                  resizeMode="cover"
                />
              ))}
            </View>
          )}
        </View>
      )}

      {/* Location Section */}
      <View style={[styles.section, { backgroundColor: isDarkMode ? '#2a2a2a' : '#ffffff' }]}>
        <Text style={[styles.sectionTitle, { color: isDarkMode ? '#fff' : '#000' }]}>
          Location
        </Text>
        <Text style={[styles.sectionText, { color: isDarkMode ? '#999' : '#666' }]}>
          {station.address}
        </Text>
        {station.geometry && station.geometry.type === 'Point' && (
          <Text style={[styles.sectionText, { color: isDarkMode ? '#999' : '#666', marginTop: 4 }]}>
            Coordinates: {station.geometry.coordinates[1].toFixed(4)}, {station.geometry.coordinates[0].toFixed(4)}
          </Text>
        )}
      </View>

      {/* Amenities Section */}
      <View style={[styles.section, { backgroundColor: isDarkMode ? '#2a2a2a' : '#ffffff' }]}>
        <Text style={[styles.sectionTitle, { color: isDarkMode ? '#fff' : '#000' }]}>
          Amenities
        </Text>
        <View style={styles.amenitiesContainer}>
          {station.amenities.map((amenity: string, index: number) => (
            <View key={index} style={[styles.amenityTag, { backgroundColor: isDarkMode ? '#333' : '#e0f2fe' }]}>
              <Text style={[styles.amenityText, { color: isDarkMode ? '#fff' : '#000' }]}>
                {amenity}
              </Text>
            </View>
          ))}
        </View>
      </View>

      {/* Operating Hours Section */}
      <View style={[styles.section, { backgroundColor: isDarkMode ? '#2a2a2a' : '#ffffff' }]}>
        <Text style={[styles.sectionTitle, { color: isDarkMode ? '#fff' : '#000' }]}>
          Operating Hours
        </Text>
        <Text style={[styles.sectionText, { color: isDarkMode ? '#999' : '#666' }]}>
          {station.operating_hours || '24/7'}
        </Text>
      </View>

      {/* Charger Details Section */}
      {station.chargers && station.chargers.length > 0 && (
        <View style={[styles.section, { backgroundColor: isDarkMode ? '#2a2a2a' : '#ffffff' }]}>
          <Text style={[styles.sectionTitle, { color: isDarkMode ? '#fff' : '#000' }]}>
            Chargers ({station.chargers.length})
          </Text>
          {station.chargers.map((charger: any, index: number) => (
            <View key={index} style={[styles.chargerItem, { backgroundColor: isDarkMode ? '#333' : '#f5f5f5' }]}>
              <View style={styles.chargerInfo}>
                <View style={styles.chargerTypeRow}>
                  <Text style={[styles.chargerType, { color: isDarkMode ? '#fff' : '#000' }]}>
                    {charger.charger_type}
                  </Text>
                  <View style={[
                    styles.chargerPowerBadge,
                    {
                      backgroundColor: charger.power_kw >= 50 ? '#3B82F6' : charger.power_kw >= 20 ? '#10B981' : '#F59E0B',
                    },
                  ]}>
                    <Text style={[styles.chargerPowerText, { color: '#fff' }]}>
                      {charger.power_kw} kW
                    </Text>
                  </View>
                </View>
                <Text style={[styles.chargerCount, { color: isDarkMode ? '#999' : '#666' }]}>
                  {charger.connector_count} connector(s)
                </Text>
                <Text style={[styles.chargerRate, { color: isDarkMode ? '#4ade80' : '#10B981' }]}>
                  Rate: {getChargeRate(charger)}
                </Text>
              </View>
              <View style={styles.chargerStatus}>
                <Text style={[
                  styles.statusText,
                  {
                    color: charger.availability_status === 'available' ? '#10B981' : charger.availability_status === 'in_use' ? '#EF4444' : '#F59E0B',
                  },
                ]}>
                  {charger.availability_status}
                </Text>
              </View>
            </View>
          ))}
        </View>
      )}

      {/* Action Buttons */}
      <View style={styles.buttonContainer}>
        <View style={styles.buttonWrapper}>
          <TouchableOpacity
            style={[styles.navigateButton, { backgroundColor: '#3B82F6' }]}
            onPress={handleNavigation}
          >
            <Text style={styles.buttonText}>Navigate</Text>
          </TouchableOpacity>
        </View>
        <View style={styles.buttonWrapper}>
          <TouchableOpacity
            style={[styles.directionsButton, { backgroundColor: isDarkMode ? '#4ade80' : '#10B981' }]}
            onPress={handleDirections}
          >
            <Text style={[styles.buttonText, { color: '#fff' }]}>Directions</Text>
          </TouchableOpacity>
        </View>
      </View>
    </ScrollView>
  )
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  header: {
    flexDirection: 'row',
    alignItems: 'center',
    padding: 16,
    borderBottomWidth: 1,
    borderBottomColor: '#ddd',
  },
  backButton: {
    fontSize: 18,
    fontWeight: 'bold',
    marginRight: 16,
    padding: 8,
  },
  title: {
    fontSize: 24,
    fontWeight: 'bold',
    flex: 1,
  },
  imagesSection: {
    borderBottomWidth: 1,
    borderBottomColor: '#ddd',
  },
  imagesHeader: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: 16,
  },
  imagesTitle: {
    fontSize: 18,
    fontWeight: 'bold',
  },
  imagesChevron: {
    fontSize: 16,
  },
  imagesGrid: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    padding: 8,
  },
  imageItem: {
    width: '33.33%',
    height: 120,
    backgroundColor: '#f5f5f5',
  },
  imageItemLast: {
    marginBottom: 8,
  },
  imageItemSingle: {
    width: '100%',
  },
  section: {
    padding: 16,
    borderBottomWidth: 1,
    borderBottomColor: '#ddd',
  },
  sectionTitle: {
    fontSize: 18,
    fontWeight: 'bold',
    marginBottom: 8,
  },
  sectionText: {
    fontSize: 14,
  },
  amenitiesContainer: {
    flexDirection: 'row',
    flexWrap: 'wrap',
    gap: 8,
  },
  amenityTag: {
    paddingHorizontal: 12,
    paddingVertical: 6,
    borderRadius: 4,
  },
  amenityText: {
    fontSize: 12,
  },
  chargerItem: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: 12,
    borderRadius: 8,
    marginBottom: 8,
  },
  chargerInfo: {
    flex: 1,
  },
  chargerTypeRow: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: 8,
    marginBottom: 4,
  },
  chargerType: {
    fontSize: 16,
    fontWeight: 'bold',
  },
  chargerPowerBadge: {
    paddingHorizontal: 8,
    paddingVertical: 4,
    borderRadius: 4,
  },
  chargerPowerText: {
    fontSize: 10,
    fontWeight: 'bold',
  },
  chargerCount: {
    fontSize: 14,
    marginBottom: 4,
  },
  chargerRate: {
    fontSize: 12,
  },
  chargerStatus: {
    backgroundColor: '#10B981',
    paddingHorizontal: 12,
    paddingVertical: 6,
    borderRadius: 4,
  },
  statusText: {
    color: '#fff',
    fontSize: 12,
    fontWeight: 'bold',
  },
  buttonContainer: {
    padding: 16,
    borderTopWidth: 1,
    borderTopColor: '#ddd',
  },
  buttonWrapper: {
    marginBottom: 12,
  },
  navigateButton: {
    padding: 16,
    borderRadius: 8,
    alignItems: 'center',
  },
  directionsButton: {
    padding: 16,
    borderRadius: 8,
    alignItems: 'center',
  },
  buttonText: {
    color: '#fff',
    fontSize: 16,
    fontWeight: 'bold',
  },
  message: {
    fontSize: 16,
    textAlign: 'center',
    marginTop: 50,
  },
})