import React from 'react'
import { View, Text, StyleSheet, TouchableOpacity } from 'react-native'
import {
  brandPrimary,
  neutral100,
  neutral600,
  fontFamilySans,
  fontSizeLg,
  fontWeightBold,
  spacing3,
  spacing4,
  radiusMd,
  shadowPanel,
} from '@borne-map/ui/src/tokens/native'

interface ZoomControlsProps {
  onZoomIn?: () => void
  onZoomOut?: () => void
}

export default function ZoomControls({ onZoomIn, onZoomOut }: ZoomControlsProps) {
  return (
    <View style={styles.container}>
      <TouchableOpacity style={styles.button} onPress={onZoomIn}>
        <Text style={styles.buttonText}>+</Text>
      </TouchableOpacity>
      <View style={styles.divider} />
      <TouchableOpacity style={styles.button} onPress={onZoomOut}>
        <Text style={styles.buttonText}>−</Text>
      </TouchableOpacity>
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    position: 'absolute',
    bottom: 120,
    right: spacing4,
    backgroundColor: neutral100,
    borderRadius: radiusMd,
    ...shadowPanel,
  },
  button: {
    width: 40,
    height: 40,
    alignItems: 'center',
    justifyContent: 'center',
  },
  divider: {
    height: 1,
    backgroundColor: '#E2E8F0',
    marginHorizontal: spacing3,
  },
  buttonText: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeLg,
    fontWeight: fontWeightBold,
    color: brandPrimary,
  },
})
