import React from 'react'
import { View, Text, StyleSheet, TouchableOpacity } from 'react-native'
import { useSafeAreaInsets } from 'react-native-safe-area-context'
import { useTranslation } from 'react-i18next'
import {
  brandPrimary,
  brandLight,
  neutral100,
  neutral600,
  neutral700,
  fontFamilySans,
  fontSizeSm,
  spacing2,
  spacing3,
  spacing4,
} from '@borne-map/ui/src/tokens/native'

interface Tab {
  key: string
  labelKey: string
  icon: string
}

const TABS: Tab[] = [
  { key: 'HomeMap', labelKey: 'nav.map', icon: '🗺️' },
  { key: 'StationList', labelKey: 'nav.stations', icon: '📋' },
  { key: 'Search', labelKey: 'nav.search', icon: '🔍' },
  { key: 'Favorites', labelKey: 'nav.favorites', icon: '⭐' },
  { key: 'Profile', labelKey: 'nav.profile', icon: '👤' },
]

interface BottomTabBarProps {
  activeTab: string
  onTabPress: (tabKey: string) => void
}

export default function BottomTabBar({ activeTab, onTabPress }: BottomTabBarProps) {
  const insets = useSafeAreaInsets()
  const { t } = useTranslation()

  return (
    <View style={[styles.container, { paddingBottom: insets.bottom + spacing2 }]}>
      {TABS.map((tab) => {
        const isActive = tab.key === activeTab
        return (
          <TouchableOpacity
            key={tab.key}
            style={styles.tab}
            onPress={() => onTabPress(tab.key)}
          >
            <Text style={styles.icon}>{tab.icon}</Text>
            <Text
              style={[
                styles.label,
                isActive && styles.labelActive,
              ]}
            >
              {t(tab.labelKey)}
            </Text>
          </TouchableOpacity>
        )
      })}
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    flexDirection: 'row',
    backgroundColor: neutral100,
    paddingTop: spacing2,
    paddingHorizontal: spacing2,
  },
  tab: {
    flex: 1,
    alignItems: 'center',
    justifyContent: 'center',
    paddingVertical: spacing2,
  },
  icon: {
    fontSize: 20,
    marginBottom: spacing2,
  },
  label: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral600,
  },
  labelActive: {
    color: brandPrimary,
    fontWeight: '600',
  },
})
