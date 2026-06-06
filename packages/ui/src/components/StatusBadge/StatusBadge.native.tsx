import React from 'react'
import type { StatusBadgeVariant, StatusBadgeState } from '../../types'
import { success, warning, error as errorColor, neutral400 } from '../../tokens/colors'
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
  available: '#d1fae5',
  'in-use': '#fef3c7',
  maintenance: '#fee2e2',
  offline: '#f1f5f9',
}

const textColors: Record<StatusBadgeVariant, string> = {
  available: '#065f46',
  'in-use': '#92400e',
  maintenance: '#991b1b',
  offline: '#475569',
}

export function StatusBadge({
  variant,
  state = 'default',
  showDot = true,
  children,
}: StatusBadgeProps) {
  return (
    <View style={styles.container}>
      {showDot && (
        <View
          style={[
            styles.dot,
            { backgroundColor: dotColors[variant] },
          ]}
          aria-hidden="true"
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
    gap: 4,
    paddingHorizontal: 8,
    paddingVertical: 4,
    backgroundColor: '#d1fae5',
    borderRadius: 6,
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
