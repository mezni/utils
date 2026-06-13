import React from 'react'
import { StyleSheet, View, Text, ViewStyle } from 'react-native'

interface SkeletonItemProps {
  style?: ViewStyle
}

export function SkeletonStationItem({ style }: SkeletonItemProps) {
  return (
    <View style={[styles.container, style]}>
      <View style={styles.skeletonLine} />
      <View style={styles.skeletonLineShort} />
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
    marginBottom: 8,
    backgroundColor: '#e5e5e5',
  },
  skeletonLineShort: {
    width: 120,
    height: 16,
    borderRadius: 4,
    marginBottom: 8,
    backgroundColor: '#e5e5e5',
  },
  skeletonLineFull: {
    width: '100%',
  },
})
