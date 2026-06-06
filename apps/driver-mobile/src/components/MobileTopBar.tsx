import React from 'react'
import { View, Text, StyleSheet } from 'react-native'
import { useSafeAreaInsets } from 'react-native-safe-area-context'
import { useTranslation } from 'react-i18next'
import {
  brandPrimary,
  neutral600,
  fontFamilySans,
  fontSizeBase,
  fontSizeLg,
  fontWeightBold,
  spacing3,
  spacing4,
} from '@borne-map/ui/src/tokens/native'

interface MobileTopBarProps {
  title?: string
}

export default function MobileTopBar({ title }: MobileTopBarProps) {
  const insets = useSafeAreaInsets()
  const { t } = useTranslation()

  return (
    <View style={[styles.container, { paddingTop: insets.top + spacing3 }]}>
      <Text style={styles.title}>
        {title ?? t('app.name')}
      </Text>
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    flexDirection: 'row',
    alignItems: 'center',
    justifyContent: 'center',
    paddingHorizontal: spacing4,
    paddingBottom: spacing3,
    backgroundColor: brandPrimary,
  },
  title: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeLg,
    fontWeight: fontWeightBold,
    color: neutral600,
  },
})
