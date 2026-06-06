import React from 'react'
import { View, Text, StyleSheet, TouchableOpacity } from 'react-native'
import {
  brandPrimary,
  neutral100,
  fontFamilySans,
  fontSizeXl,
  spacing4,
  radiusFull,
  shadowFloat,
} from '@borne-map/ui/src/tokens/native'

interface CenterActionButtonProps {
  onPress?: () => void
}

export default function CenterActionButton({ onPress }: CenterActionButtonProps) {
  return (
    <TouchableOpacity style={styles.container} onPress={onPress} activeOpacity={0.8}>
      <Text style={styles.icon}>⚡</Text>
    </TouchableOpacity>
  )
}

const styles = StyleSheet.create({
  container: {
    position: 'absolute',
    bottom: 60,
    alignSelf: 'center',
    width: 56,
    height: 56,
    borderRadius: radiusFull,
    backgroundColor: brandPrimary,
    alignItems: 'center',
    justifyContent: 'center',
    ...shadowFloat,
  },
  icon: {
    fontSize: fontSizeXl,
  },
})
