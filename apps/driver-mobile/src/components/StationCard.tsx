import React from 'react'
import { View, Text, StyleSheet, TouchableOpacity } from 'react-native'
import { useTranslation } from 'react-i18next'
import { StatusBadge } from '@borne-map/ui'
import { neutral100, neutral700, neutral400, neutral600, neutral300 } from '@borne-map/ui/tokens/colors'
import { radiusMd, radiusFull } from '@borne-map/ui/tokens/radius'
import { spacing4, spacing1, spacing2 } from '@borne-map/ui/tokens/spacing'
import { fontSizeBase, fontSizeSm } from '@borne-map/ui/tokens/typography'
import { fontWeightBold } from '@borne-map/ui/tokens/typography'
import { fontFamilySans } from '@borne-map/ui/tokens/typography'
import { shadowCard } from '@borne-map/ui/tokens/shadows'

interface StationCardProps {
  name: string
  address: string
  distance: number
  chargerCount: number
  availableCount: number
  availability: 'available' | 'unavailable'
  rating: number
  isFavorite?: boolean
  onFavoritePress?: () => void
  onPress?: () => void
}

export default function StationCard({
  name,
  address,
  distance,
  chargerCount,
  availableCount,
  availability,
  rating,
  isFavorite,
  onFavoritePress,
  onPress,
}: StationCardProps) {
  const { t } = useTranslation()
  const isAvail = availability === 'available'

  return (
    <TouchableOpacity style={styles.container} onPress={onPress} activeOpacity={0.7}>
      <View style={styles.header}>
        <Text style={styles.name} numberOfLines={1}>{name}</Text>
        <TouchableOpacity onPress={onFavoritePress} hitSlop={{ top: 8, bottom: 8, left: 8, right: 8 }}>
          <Text style={styles.heart}>{isFavorite ? '❤️' : '🤍'}</Text>
        </TouchableOpacity>
      </View>
      <Text style={styles.address} numberOfLines={1}>{address}</Text>
      <View style={styles.metaRow}>
        <Text style={styles.metaText}>{distance} {t('station.distance')}</Text>
        <Text style={styles.bullet}>·</Text>
        <Text style={styles.metaText}>{availableCount}/{chargerCount} {t('station.chargers')}</Text>
      </View>
      <View style={styles.bottomRow}>
        <StatusBadge variant={availability}>
          {availability === 'available' ? t('station.available') : t('station.unavailable')}
        </StatusBadge>
        <Text style={styles.rating}>⭐ {rating.toFixed(1)}</Text>
      </View>
    </TouchableOpacity>
  )
}

const styles = StyleSheet.create({
  container: {
    backgroundColor: neutral100,
    borderRadius: radiusMd,
    padding: spacing4,
    marginHorizontal: spacing4,
    marginVertical: spacing1,
    ...shadowCard,
  },
  header: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
  },
  name: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeBase,
    fontWeight: fontWeightBold,
    color: neutral700,
    flex: 1,
    marginRight: spacing2,
  },
  heart: {
    fontSize: 18,
  },
  address: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral400,
    marginTop: spacing1,
  },
  metaRow: {
    flexDirection: 'row',
    alignItems: 'center',
    marginTop: spacing2,
  },
  metaText: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral600,
  },
  bullet: {
    marginHorizontal: spacing1,
    color: neutral300,
  },
  bottomRow: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    marginTop: spacing2,
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
  },
  rating: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral600,
  },
})
