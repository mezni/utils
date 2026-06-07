import React from 'react'
import { View, Text, StyleSheet } from 'react-native'
import type { StatusBadgeVariant, StatusBadgeState } from '../../types'
import { success, warning, error as errorColor, neutral400, bgSuccess, bgWarning, bgError, bgNeutral400, textSuccess, textWarning, textError, textNeutral400 } from '../../tokens/colors'
import { fontSizeSm, fontWeightMedium } from '../../tokens/typography'
import { spacing1, spacing2 } from '../../tokens/spacing'
import { radiusSm } from '../../tokens/radius'

interface StatusBadgeProps {
  variant: StatusBadgeVariant
  state?: StatusBadgeState
  showDot?: boolean
  children?: React.ReactNode
}

const dotColors: Record<StatusBadgeVariant, string> = {
  available: success,
  'in-use': warning,
  maintenance: errorColor,
  offline: neutral400,
}

const bgColors: Record<StatusBadgeVariant, string> = {
  available: bgSuccess,
  'in-use': bgWarning,
  maintenance: bgError,
  offline: bgNeutral400,
}

const textColors: Record<StatusBadgeVariant, string> = {
  available: textSuccess,
  'in-use': textWarning,
  maintenance: textError,
  offline: textNeutral400,
}

export function StatusBadge({
  variant,
  state = 'default',
  showDot = true,
  children,
}: StatusBadgeProps) {
  return (
    <View style={[styles.container, { backgroundColor: bgColors[variant] }]}>
      {showDot && (
        <View
          style={[
            styles.dot,
            { backgroundColor: dotColors[variant] },
          ]}
          accessibilityElementsHidden={true}
        />
      )}
      <Text style={[styles.text, { color: textColors[variant] }]}>
        {children}
      </Text>
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    flexDirection: 'row',
    alignItems: 'center',
    gap: spacing1,
    paddingHorizontal: spacing2,
    paddingVertical: spacing1,
    borderRadius: radiusSm,
    minHeight: 24,
  },
  dot: {
    width: 8,
    height: 8,
    borderRadius: 4,
    flexShrink: 0,
  },
  text: {
    fontSize: fontSizeSm,
    fontWeight: fontWeightMedium,
    lineHeight: 1,
  },
})
