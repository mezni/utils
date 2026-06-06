import React, { useMemo } from 'react'
import { View, Text, FlatList, ScrollView, StyleSheet } from 'react-native'
import { useRoute, useNavigation } from '@react-navigation/native'
import type { RouteProp } from '@react-navigation/native'
import type { NativeStackNavigationProp } from '@react-navigation/native-stack'
import { useTranslation } from 'react-i18next'
import {
  brandPrimary,
  brandLight,
  neutral400,
  neutral600,
  neutral700,
  fontFamilySans,
  fontSizeSm,
  fontSizeBase,
  fontSizeLg,
  fontWeightBold,
  fontWeightMedium,
  spacing2,
  spacing3,
  spacing4,
} from '@borne-map/ui/src/tokens/native'
import type { RootStackParamList } from '../navigation/types'
import type { Charger, Review } from '../types'
import { useStations } from '../hooks/useStations'
import { chargers } from '../mocks/chargers'
import { reviews } from '../mocks/reviews'
import ChargerRow from '../components/ChargerRow'
import ReviewCard from '../components/ReviewCard'

type DetailRouteProp = RouteProp<RootStackParamList, 'StationDetail'>

function EmptyState({ message }: { message: string }) {
  return (
    <View style={styles.emptyState}>
      <Text style={styles.emptyText}>{message}</Text>
    </View>
  )
}

export default function StationDetailScreen() {
  const { t } = useTranslation()
  const route = useRoute<DetailRouteProp>()
  const navigation = useNavigation<NativeStackNavigationProp<RootStackParamList>>()
  const { stationId } = route.params
  const { getStationById } = useStations()

  const station = getStationById(stationId)

  const stationChargers = useMemo(
    () => chargers.filter((c) => c.stationId === stationId),
    [stationId],
  )

  const stationReviews = useMemo(
    () => reviews.filter((r) => r.stationId === stationId),
    [stationId],
  )

  if (!station) {
    return (
      <View style={styles.container}>
        <EmptyState message={t('common.error')} />
      </View>
    )
  }

  return (
    <ScrollView style={styles.container} contentContainerStyle={styles.content}>
      <View style={styles.header}>
        <Text style={styles.name}>{station.name}</Text>
        <Text style={styles.address}>{station.address}</Text>
        <View style={styles.statsRow}>
          <Text style={styles.stat}>⭐ {station.rating.toFixed(1)}</Text>
          <Text style={styles.statDot}>·</Text>
          <Text style={styles.stat}>{station.distance} {t('station.distance')}</Text>
          <Text style={styles.statDot}>·</Text>
          <Text style={styles.stat}>{station.chargerCount} {t('station.chargers')}</Text>
        </View>
      </View>

      <Text style={styles.sectionTitle}>{t('station.chargers')}</Text>
      {stationChargers.length > 0 ? (
        <FlatList
          data={stationChargers}
          keyExtractor={(item: Charger) => item.id}
          renderItem={({ item }) => (
            <ChargerRow
              connectorType={item.connectorType}
              powerKw={item.powerKw}
              pricePerKwh={item.pricePerKwh}
              availability={item.availability}
            />
          )}
          scrollEnabled={false}
        />
      ) : (
        <EmptyState message={t('station.noChargers')} />
      )}

      <Text style={styles.sectionTitle}>{t('station.reviews')}</Text>
      {stationReviews.length > 0 ? (
        <FlatList
          data={stationReviews}
          keyExtractor={(item: Review) => item.id}
          renderItem={({ item }) => (
            <ReviewCard
              authorName={item.authorName}
              rating={item.rating}
              text={item.text}
              date={item.date}
            />
          )}
          scrollEnabled={false}
        />
      ) : (
        <EmptyState message={t('station.noReviews')} />
      )}

      <View style={styles.bottomSpacer} />
    </ScrollView>
  )
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: brandLight,
  },
  content: {
    paddingTop: spacing4,
  },
  header: {
    paddingHorizontal: spacing4,
    marginBottom: spacing4,
  },
  name: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeLg,
    fontWeight: fontWeightBold,
    color: neutral700,
  },
  address: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral400,
    marginTop: spacing2,
  },
  statsRow: {
    flexDirection: 'row',
    alignItems: 'center',
    marginTop: spacing3,
  },
  stat: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral600,
  },
  statDot: {
    marginHorizontal: spacing2,
    color: neutral400,
  },
  sectionTitle: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeBase,
    fontWeight: fontWeightBold,
    color: neutral700,
    paddingHorizontal: spacing4,
    marginTop: spacing4,
    marginBottom: spacing2,
  },
  emptyState: {
    alignItems: 'center',
    paddingVertical: spacing4,
    paddingHorizontal: spacing4,
  },
  emptyText: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral400,
  },
  bottomSpacer: {
    height: spacing4,
  },
})
