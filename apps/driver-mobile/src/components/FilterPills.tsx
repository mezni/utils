import React from 'react'
import { View, Text, ScrollView, StyleSheet, TouchableOpacity } from 'react-native'
import {
  brandPrimary,
  brandLight,
  neutral100,
  neutral400,
  neutral600,
  neutral700,
  fontFamilySans,
  fontSizeSm,
  spacing1,
  spacing2,
  spacing3,
  radiusFull,
} from '@borne-map/ui/src/tokens/native'

type ConnectorType = 'all' | 'Type2' | 'CCS' | 'CHAdeMO'

interface FilterPillsProps {
  selectedType: ConnectorType
  onTypeChange: (type: ConnectorType) => void
  showAvailabilityFilter?: boolean
  availabilityFilter: 'all' | 'available'
  onAvailabilityChange: (value: 'all' | 'available') => void
}

const TYPES: { key: ConnectorType; label: string }[] = [
  { key: 'all', label: 'Tous' },
  { key: 'Type2', label: 'Type 2' },
  { key: 'CCS', label: 'CCS' },
  { key: 'CHAdeMO', label: 'CHAdeMO' },
]

export default function FilterPills({
  selectedType,
  onTypeChange,
  showAvailabilityFilter = true,
  availabilityFilter,
  onAvailabilityChange,
}: FilterPillsProps) {
  return (
    <ScrollView
      horizontal
      showsHorizontalScrollIndicator={false}
      contentContainerStyle={styles.container}
    >
      {TYPES.map((type) => (
        <TouchableOpacity
          key={type.key}
          style={[
            styles.pill,
            selectedType === type.key && styles.pillActive,
          ]}
          onPress={() => onTypeChange(type.key)}
        >
          <Text
            style={[
              styles.pillText,
              selectedType === type.key && styles.pillTextActive,
            ]}
          >
            {type.label}
          </Text>
        </TouchableOpacity>
      ))}
      {showAvailabilityFilter && (
        <TouchableOpacity
          style={[
            styles.pill,
            availabilityFilter === 'available' && styles.pillActive,
          ]}
          onPress={() =>
            onAvailabilityChange(
              availabilityFilter === 'available' ? 'all' : 'available',
            )
          }
        >
          <Text
            style={[
              styles.pillText,
              availabilityFilter === 'available' && styles.pillTextActive,
            ]}
          >
            {availabilityFilter === 'available' ? 'Disponible' : 'Tous'}
          </Text>
        </TouchableOpacity>
      )}
    </ScrollView>
  )
}

const styles = StyleSheet.create({
  container: {
    paddingHorizontal: spacing3,
    paddingVertical: spacing2,
    gap: spacing2,
  },
  pill: {
    paddingHorizontal: spacing3,
    paddingVertical: spacing1 + 2,
    borderRadius: radiusFull,
    backgroundColor: neutral100,
    marginRight: spacing2,
  },
  pillActive: {
    backgroundColor: brandPrimary,
  },
  pillText: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral600,
  },
  pillTextActive: {
    color: '#FFFFFF',
  },
})
