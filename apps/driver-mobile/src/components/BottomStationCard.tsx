import React from 'react'
import { View, Text, StyleSheet, TouchableOpacity } from 'react-native'
import { useTranslation } from 'react-i18next'
import {
  brandPrimary,
  success,
  error,
  neutral100,
  neutral400,
  neutral600,
  neutral700,
  fontFamilySans,
  fontSizeSm,
  fontSizeBase,
  fontWeightBold,
  fontWeightMedium,
  spacing1,
  spacing2,
  spacing3,
  spacing4,
  radiusMd,
  radiusFull,
  shadowFloat,
} from '@borne-map/ui/src/tokens/native'

interface BottomStationCardProps {
  name: string
  address: string
  availability: 'available' | 'unavailable'
  distance: number
  chargerCount: number
  rating: number
  onPress?: () => void
}

export default function BottomStationCard({
  name,
  address,
  availability,
  distance,
  chargerCount,
  rating,
  onPress,
}: BottomStationCardProps) {
  const { t } = useTranslation()
  const isAvailable = availability === 'available'

  return (
    <TouchableOpacity
      style={styles.container}
      onPress={onPress}
      activeOpacity={0.9}
    >
      <View style={styles.header}>
        <Text style={styles.name} numberOfLines={1}>
          {name}
        </Text>
        <View
          style={[
            styles.badge,
            { backgroundColor: isAvailable ? success : error },
          ]}
        >
          <Text style={styles.badgeText}>
            {isAvailable ? t('station.available') : t('station.unavailable')}
          </Text>
        </View>
      </View>
      <Text style={styles.address} numberOfLines={1}>
        {address}
      </Text>
      <View style={styles.meta}>
        <Text style={styles.metaText}>
          {distance} {t('station.distance')}
        </Text>
        <Text style={styles.metaDot}>·</Text>
        <Text style={styles.metaText}>
          {chargerCount} {t('station.chargers')}
        </Text>
        <Text style={styles.metaDot}>·</Text>
        <Text style={styles.metaText}>
          ⭐ {rating.toFixed(1)}
        </Text>
      </View>
    </TouchableOpacity>
  )
}

const styles = StyleSheet.create({
  container: {
    position: 'absolute',
    bottom: 90,
    left: spacing4,
    right: spacing4,
    backgroundColor: neutral100,
    borderRadius: radiusMd,
    padding: spacing4,
    ...shadowFloat,
  },
  header: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginBottom: spacing1,
  },
  name: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeBase,
    fontWeight: fontWeightBold,
    color: neutral700,
    flex: 1,
    marginRight: spacing2,
  },
  badge: {
    paddingHorizontal: spacing2,
    paddingVertical: spacing1 / 2,
    borderRadius: radiusFull,
  },
  badgeText: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: '#FFFFFF',
    fontWeight: fontWeightMedium,
  },
  address: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral400,
    marginBottom: spacing2,
  },
  meta: {
    flexDirection: 'row',
    alignItems: 'center',
  },
  metaText: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral600,
  },
  metaDot: {
    marginHorizontal: spacing1,
    color: neutral400,
  },
})
