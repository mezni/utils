import React from 'react'
import { View, Text, StyleSheet } from 'react-native'
import {
  neutral400,
  neutral600,
  fontFamilySans,
  fontSizeSm,
  fontWeightMedium,
  spacing1,
  spacing2,
  spacing3,
} from '@borne-map/ui/src/tokens/native'

interface SpecRowProps {
  label: string
  value: string
}

export default function SpecRow({ label, value }: SpecRowProps) {
  return (
    <View style={styles.container}>
      <Text style={styles.label}>{label}</Text>
      <Text style={styles.value}>{value}</Text>
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    flexDirection: 'row',
    justifyContent: 'space-between',
    alignItems: 'center',
    paddingVertical: spacing2,
  },
  label: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral400,
  },
  value: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    fontWeight: fontWeightMedium,
    color: neutral600,
  },
})
