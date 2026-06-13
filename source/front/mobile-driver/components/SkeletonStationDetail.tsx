import React from 'react'
import { StyleSheet, View, ViewStyle } from 'react-native'

interface SkeletonDetailProps {
  style?: ViewStyle
}

export function SkeletonStationDetail({ style }: SkeletonDetailProps) {
  return (
    <View style={[styles.container, style]}>
      <View style={styles.skeletonLine} />
      <View style={[styles.skeletonLine, styles.skeletonLineFull]} />
      <View style={[styles.skeletonLine, styles.skeletonLineFull]} />
      <View style={[styles.skeletonLine, styles.skeletonLineFull]} />
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    padding: 16,
    borderRadius: 8,
    marginBottom: 12,
    backgroundColor: '#f5f5f5',
  },
  skeletonLine: {
    height: 16,
    borderRadius: 4,
    marginBottom: 12,
    backgroundColor: '#e5e5e5',
  },
  skeletonLineFull: {
    width: '100%',
  },
})
