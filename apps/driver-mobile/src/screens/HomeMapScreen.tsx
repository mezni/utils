import React, { useMemo, useCallback } from 'react'
import {
  View,
  StyleSheet,
  Dimensions,
} from 'react-native'
import { useNavigation } from '@react-navigation/native'
import type { NativeStackNavigationProp } from '@react-navigation/native-stack'
import { useTranslation } from 'react-i18next'
import { brandLight } from '@borne-map/ui/src/tokens/native'
import type { RootStackParamList } from '../navigation/types'
import { useStations } from '../hooks/useStations'
import SearchBar from '../components/SearchBar'
import FilterPills from '../components/FilterPills'
import MapPinMarker from '../components/MapPinMarker'
import BottomStationCard from '../components/BottomStationCard'
import ZoomControls from '../components/ZoomControls'
import CenterActionButton from '../components/CenterActionButton'

type NavigationProp = NativeStackNavigationProp<RootStackParamList>

const { width: SCREEN_WIDTH, height: SCREEN_HEIGHT } = Dimensions.get('window')
const MAP_HEIGHT = SCREEN_HEIGHT * 0.65
const PIN_AREA_WIDTH = SCREEN_WIDTH * 0.86
const PIN_AREA_HEIGHT = MAP_HEIGHT * 0.86

const LAT_MIN = 36.79
const LAT_MAX = 36.88
const LNG_MIN = 10.13
const LNG_MAX = 10.25

function toPosition(lat: number, lng: number) {
  const x = ((lng - LNG_MIN) / (LNG_MAX - LNG_MIN)) * PIN_AREA_WIDTH
  const y = ((LAT_MAX - lat) / (LAT_MAX - LAT_MIN)) * PIN_AREA_HEIGHT
  return {
    left: Math.max(0, Math.min(PIN_AREA_WIDTH, x)),
    top: Math.max(0, Math.min(PIN_AREA_HEIGHT, y)),
  }
}

export default function HomeMapScreen() {
  const { t } = useTranslation()
  const navigation = useNavigation<NavigationProp>()
  const { allStations } = useStations()
  const [selectedStationId, setSelectedStationId] = React.useState<string | null>(null)
  const [typeFilter, setTypeFilter] = React.useState<'all' | 'Type2' | 'CCS' | 'CHAdeMO'>('all')
  const [availFilter, setAvailFilter] = React.useState<'all' | 'available'>('all')

  const selectedStation = useMemo(
    () => allStations.find((s) => s.id === selectedStationId) ?? allStations[0],
    [allStations, selectedStationId],
  )

  const handlePinPress = useCallback((id: string) => {
    setSelectedStationId(id)
  }, [])

  const handleStationCardPress = useCallback(() => {
    if (selectedStation) {
      navigation.navigate('StationDetail', { stationId: selectedStation.id })
    }
  }, [navigation, selectedStation])

  const handleSearchFocus = useCallback(() => {
    navigation.navigate('MainTabs')
  }, [navigation])

  return (
    <View style={styles.container}>
      <View style={styles.mapBackground}>
        {allStations.map((station) => {
          const pos = toPosition(station.coordinates.lat, station.coordinates.lng)
          return (
            <View
              key={station.id}
              style={[
                styles.pinWrapper,
                { left: pos.left, top: pos.top },
              ]}
            >
              <MapPinMarker
                state={
                  station.id === selectedStationId
                    ? 'selected'
                    : station.availability === 'unavailable'
                    ? 'unavailable'
                    : 'default'
                }
                onPress={() => handlePinPress(station.id)}
              />
            </View>
          )
        })}
      </View>

      <View style={styles.searchOverlay}>
        <SearchBar onFocus={handleSearchFocus} />
      </View>

      <View style={styles.filtersOverlay}>
        <FilterPills
          selectedType={typeFilter}
          onTypeChange={setTypeFilter}
          showAvailabilityFilter={false}
          availabilityFilter={availFilter}
          onAvailabilityChange={setAvailFilter}
        />
      </View>

      <ZoomControls />

      {selectedStation && (
        <BottomStationCard
          name={selectedStation.name}
          address={selectedStation.address}
          availability={selectedStation.availability}
          distance={selectedStation.distance}
          chargerCount={selectedStation.chargerCount}
          rating={selectedStation.rating}
          onPress={handleStationCardPress}
        />
      )}

      <CenterActionButton />
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
  mapBackground: {
    flex: 1,
    backgroundColor: brandLight,
    position: 'relative',
  },
  pinWrapper: {
    position: 'absolute',
  },
  searchOverlay: {
    position: 'absolute',
    top: 50,
    left: 0,
    right: 0,
    zIndex: 10,
  },
  filtersOverlay: {
    position: 'absolute',
    top: 110,
    left: 0,
    right: 0,
    zIndex: 9,
  },
})
