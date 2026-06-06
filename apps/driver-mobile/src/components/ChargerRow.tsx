import React from 'react'
import { View, Text, StyleSheet } from 'react-native'
import { useTranslation } from 'react-i18next'
import {
  success,
  error,
  neutral100,
  neutral400,
  neutral600,
  neutral700,
  fontFamilySans,
  fontSizeSm,
  fontSizeBase,
  fontWeightMedium,
  spacing2,
  spacing3,
  spacing4,
  radiusMd,
  radiusFull,
  shadowCard,
} from '@borne-map/ui/src/tokens/native'

interface ChargerRowProps {
  connectorType: 'Type2' | 'CCS' | 'CHAdeMO'
  powerKw: number
  pricePerKwh: number
  availability: 'available' | 'unavailable'
}

export default function ChargerRow({
  connectorType,
  powerKw,
  pricePerKwh,
  availability,
}: ChargerRowProps) {
  const { t } = useTranslation()
  const isAvailable = availability === 'available'

  return (
    <View style={styles.container}>
      <View style={styles.left}>
        <Text style={styles.type}>{connectorType}</Text>
        <Text style={styles.power}>{powerKw} kW</Text>
      </View>
      <View style={styles.right}>
        <Text style={styles.price}>{pricePerKwh.toFixed(3)} {t('station.pricePerKwh')}</Text>
        <View style={[styles.badge, { backgroundColor: isAvailable ? success : error }]}>
          <Text style={styles.badgeText}>
            {isAvailable ? t('station.available') : t('station.unavailable')}
          </Text>
        </View>
      </View>
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    backgroundColor: neutral100,
    borderRadius: radiusMd,
    padding: spacing4,
    marginHorizontal: spacing4,
    marginVertical: spacing2,
    ...shadowCard,
  },
  left: {
    flex: 1,
  },
  type: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeBase,
    fontWeight: fontWeightMedium,
    color: neutral700,
  },
  power: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral400,
    marginTop: spacing2,
  },
  right: {
    alignItems: 'flex-end',
  },
  price: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral600,
    marginBottom: spacing2,
  },
  badge: {
    paddingHorizontal: spacing2,
    paddingVertical: spacing2 / 2,
    borderRadius: radiusFull,
  },
  badgeText: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: '#FFFFFF',
  },
})
