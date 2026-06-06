import React, { useCallback } from 'react'
import { View, Text, FlatList, StyleSheet } from 'react-native'
import { useNavigation } from '@react-navigation/native'
import type { NativeStackNavigationProp } from '@react-navigation/native-stack'
import { useTranslation } from 'react-i18next'
import {
  brandLight,
  neutral400,
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

export default function FavoritesScreen() {
  const { t } = useTranslation()
  const navigation = useNavigation<NavigationProp>()
  const { allStations } = useStations()
  const { favoriteStationIds, isFavorite, toggleFavorite } = useFavorites()

  const favoriteStations = allStations.filter((s) =>
    favoriteStationIds.includes(s.id),
  )

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

  return (
    <View style={styles.container}>
      <FlatList
        data={favoriteStations}
        keyExtractor={(item) => item.id}
        renderItem={renderStation}
        ListEmptyComponent={
          <View style={styles.emptyState}>
            <Text style={styles.emptyText}>{t('favorites.empty')}</Text>
          </View>
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
  list: {
    paddingTop: spacing4,
    paddingBottom: spacing4,
  },
  emptyState: {
    alignItems: 'center',
    paddingVertical: 60,
    paddingHorizontal: spacing4,
  },
  emptyText: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeBase,
    color: neutral400,
  },
})
