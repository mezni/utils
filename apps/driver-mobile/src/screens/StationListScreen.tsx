import React, { useCallback, useMemo } from 'react'
import {
  View,
  Text,
  FlatList,
  StyleSheet,
  ActivityIndicator,
  RefreshControl,
} from 'react-native'
import { useNavigation } from '@react-navigation/native'
import type { NativeStackNavigationProp } from '@react-navigation/native-stack'
import { useTranslation } from 'react-i18next'
import {
  brandPrimary,
  brandLight,
  neutral400,
  neutral600,
  fontFamilySans,
  fontSizeBase,
  spacing4,
} from '@borne-map/ui/src/tokens/native'
import type { RootStackParamList } from '../navigation/types'
import type { Station } from '../types'
import { useStations } from '../hooks/useStations'
import { useFavorites } from '../hooks/useFavorites'
import StationCard from '../components/StationCard'

type NavigationProp = NativeStackNavigationProp<RootStackParamList>

export default function StationListScreen() {
  const { t } = useTranslation()
  const navigation = useNavigation<NavigationProp>()
  const { allStations } = useStations()
  const { isFavorite, toggleFavorite } = useFavorites()
  const [refreshing, setRefreshing] = React.useState(false)
  const [loading, setLoading] = React.useState(true)

  React.useEffect(() => {
    const timer = setTimeout(() => setLoading(false), 500)
    return () => clearTimeout(timer)
  }, [])

  const onRefresh = useCallback(() => {
    setRefreshing(true)
    setTimeout(() => setRefreshing(false), 1000)
  }, [])

  const handleStationPress = useCallback(
    (stationId: string) => {
      navigation.navigate('StationDetail', { stationId })
    },
    [navigation],
  )

  const renderStation = useCallback(
    ({ item }: { item: Station }) => (
      <StationCard
        name={item.name}
        address={item.address}
        distance={item.distance}
        chargerCount={item.chargerCount}
        availableCount={item.availableCount}
        availability={item.availability}
        rating={item.rating}
        isFavorite={isFavorite(item.id)}
        onFavoritePress={() => toggleFavorite(item.id)}
        onPress={() => handleStationPress(item.id)}
      />
    ),
    [isFavorite, toggleFavorite, handleStationPress],
  )

  if (loading) {
    return (
      <View style={styles.center}>
        <View style={styles.skeleton}>
          {[1, 2, 3, 4].map((i) => (
            <View key={i} style={styles.skeletonCard} />
          ))}
        </View>
      </View>
    )
  }

  return (
    <View style={styles.container}>
      <Text style={styles.title}>{t('home.title')}</Text>
      <FlatList
        data={allStations}
        keyExtractor={(item) => item.id}
        renderItem={renderStation}
        refreshControl={
          <RefreshControl refreshing={refreshing} onRefresh={onRefresh} />
        }
        contentContainerStyle={styles.list}
        showsVerticalScrollIndicator={false}
      />
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: brandLight,
  },
  center: {
    flex: 1,
    backgroundColor: brandLight,
  },
  title: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeBase,
    color: neutral600,
    paddingHorizontal: spacing4,
    paddingTop: spacing4,
    paddingBottom: spacing4,
  },
  list: {
    paddingBottom: spacing4,
  },
  skeleton: {
    paddingHorizontal: spacing4,
    paddingTop: spacing4,
  },
  skeletonCard: {
    height: 120,
    backgroundColor: '#E2E8F0',
    borderRadius: 12,
    marginBottom: 12,
    opacity: 0.6,
  },
})
