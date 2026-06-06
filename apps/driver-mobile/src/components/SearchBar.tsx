import React from 'react'
import {
  View,
  TextInput,
  StyleSheet,
  TouchableOpacity,
  Text,
} from 'react-native'
import { useTranslation } from 'react-i18next'
import {
  brandPrimary,
  brandLight,
  neutral100,
  neutral400,
  fontFamilySans,
  fontSizeBase,
  spacing2,
  spacing3,
  spacing4,
  radiusLg,
  shadowCard,
} from '@borne-map/ui/src/tokens/native'

interface SearchBarProps {
  value?: string
  onChangeText?: (text: string) => void
  onFocus?: () => void
  placeholder?: string
  editable?: boolean
}

export default function SearchBar({
  value,
  onChangeText,
  onFocus,
  placeholder,
  editable = true,
}: SearchBarProps) {
  const { t } = useTranslation()

  return (
    <View style={styles.container}>
      <View style={styles.inputWrapper}>
        <Text style={styles.icon}>🔍</Text>
        <TextInput
          style={styles.input}
          value={value}
          onChangeText={onChangeText}
          onFocus={onFocus}
          placeholder={placeholder ?? t('home.searchPlaceholder')}
          placeholderTextColor={neutral400}
          editable={editable}
        />
      </View>
    </View>
  )
}

const styles = StyleSheet.create({
  container: {
    paddingHorizontal: spacing4,
    paddingVertical: spacing2,
  },
  inputWrapper: {
    flexDirection: 'row',
    alignItems: 'center',
    backgroundColor: neutral100,
    borderRadius: radiusLg,
    paddingHorizontal: spacing3,
    paddingVertical: spacing2,
    ...shadowCard,
  },
  icon: {
    marginRight: spacing2,
    fontSize: fontSizeBase,
  },
  input: {
    flex: 1,
    fontFamily: fontFamilySans,
    fontSize: fontSizeBase,
    color: brandPrimary,
  },
})
