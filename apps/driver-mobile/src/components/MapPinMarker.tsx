import React from 'react'
import { View, StyleSheet, TouchableOpacity } from 'react-native'
import {
  brandPrimary,
  error,
  neutral100,
  neutral400,
  spacing2,
  shadowPin,
} from '@borne-map/ui/src/tokens/native'

type PinState = 'default' | 'selected' | 'unavailable'

interface MapPinMarkerProps {
  state?: PinState
  onPress?: () => void
}

export default function MapPinMarker({ state = 'default', onPress }: MapPinMarkerProps) {
  return (
    <TouchableOpacity onPress={onPress} activeOpacity={0.7}>
      <View
        style={[
          styles.pin,
          state === 'selected' && styles.pinSelected,
          state === 'unavailable' && styles.pinUnavailable,
          state === 'default' && styles.pinDefault,
        ]}
      />
    </TouchableOpacity>
  )
}

const styles = StyleSheet.create({
  pin: {
    width: 16,
    height: 16,
    borderRadius: 8,
    borderWidth: 2,
    borderColor: '#FFFFFF',
    ...shadowPin,
  },
  pinDefault: {
    backgroundColor: brandPrimary,
  },
  pinSelected: {
    backgroundColor: '#FFFFFF',
    borderColor: brandPrimary,
    width: 20,
    height: 20,
    borderRadius: 10,
  },
  pinUnavailable: {
    backgroundColor: error,
  },
})
