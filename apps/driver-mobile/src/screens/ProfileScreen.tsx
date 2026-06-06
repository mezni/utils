import React from 'react'
import { View, Text, ScrollView, TextInput, StyleSheet, TouchableOpacity } from 'react-native'
import { useNavigation } from '@react-navigation/native'
import type { NativeStackNavigationProp } from '@react-navigation/native-stack'
import { useTranslation } from 'react-i18next'
import {
  brandPrimary,
  brandLight,
  neutral100,
  neutral200,
  neutral400,
  neutral600,
  neutral700,
  fontFamilySans,
  fontSizeSm,
  fontSizeBase,
  fontWeightBold,
  fontWeightMedium,
  spacing2,
  spacing3,
  spacing4,
  radiusMd,
  radiusLg,
  shadowCard,
} from '@borne-map/ui/src/tokens/native'
import type { RootStackParamList } from '../navigation/types'
import { users } from '../mocks/users'

type NavigationProp = NativeStackNavigationProp<RootStackParamList>

export default function ProfileScreen() {
  const { t } = useTranslation()
  const navigation = useNavigation<NavigationProp>()
  const user = users[0]

  return (
    <ScrollView style={styles.container} contentContainerStyle={styles.content}>
      <View style={styles.avatarContainer}>
        <View style={styles.avatar}>
          <Text style={styles.avatarText}>
            {user.name.charAt(0).toUpperCase()}
          </Text>
        </View>
        <Text style={styles.name}>{user.name}</Text>
      </View>

      <View style={styles.form}>
        <Text style={styles.label}>{t('profile.name')}</Text>
        <TextInput style={styles.input} value={user.name} editable={false} />

        <Text style={styles.label}>{t('profile.email')}</Text>
        <TextInput style={styles.input} value={user.email} editable={false} />

        <Text style={styles.label}>{t('profile.phone')}</Text>
        <TextInput style={styles.input} value={user.phone} editable={false} />
      </View>

      <TouchableOpacity
        style={styles.loginButton}
        onPress={() => navigation.navigate('LoginRegister')}
      >
        <Text style={styles.loginButtonText}>{t('auth.login')}</Text>
      </TouchableOpacity>
    </ScrollView>
  )
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
    backgroundColor: brandLight,
  },
  content: {
    padding: spacing4,
  },
  avatarContainer: {
    alignItems: 'center',
    marginBottom: spacing4,
  },
  avatar: {
    width: 80,
    height: 80,
    borderRadius: 40,
    backgroundColor: brandPrimary,
    alignItems: 'center',
    justifyContent: 'center',
    marginBottom: spacing3,
  },
  avatarText: {
    fontFamily: fontFamilySans,
    fontSize: 32,
    fontWeight: fontWeightBold,
    color: '#FFFFFF',
  },
  name: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeBase,
    fontWeight: fontWeightBold,
    color: neutral700,
  },
  form: {
    marginBottom: spacing4,
  },
  label: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral600,
    marginBottom: spacing2,
    marginTop: spacing3,
  },
  input: {
    backgroundColor: neutral100,
    borderRadius: radiusMd,
    padding: spacing4,
    fontFamily: fontFamilySans,
    fontSize: fontSizeBase,
    color: neutral700,
    borderWidth: 1,
    borderColor: neutral200,
  },
  loginButton: {
    backgroundColor: brandPrimary,
    borderRadius: radiusLg,
    padding: spacing4,
    alignItems: 'center',
    marginTop: spacing4,
  },
  loginButtonText: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeBase,
    fontWeight: fontWeightMedium,
    color: '#FFFFFF',
  },
})
