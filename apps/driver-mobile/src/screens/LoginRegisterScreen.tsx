import React, { useState } from 'react'
import {
  View,
  Text,
  TextInput,
  ScrollView,
  StyleSheet,
  TouchableOpacity,
} from 'react-native'
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
} from '@borne-map/ui/src/tokens/native'

export default function LoginRegisterScreen() {
  const { t } = useTranslation()
  const [isLogin, setIsLogin] = useState(true)

  return (
    <ScrollView style={styles.container} contentContainerStyle={styles.content}>
      <View style={styles.tabs}>
        <TouchableOpacity
          style={[styles.tab, isLogin && styles.tabActive]}
          onPress={() => setIsLogin(true)}
        >
          <Text style={[styles.tabText, isLogin && styles.tabTextActive]}>
            {t('auth.login')}
          </Text>
        </TouchableOpacity>
        <TouchableOpacity
          style={[styles.tab, !isLogin && styles.tabActive]}
          onPress={() => setIsLogin(false)}
        >
          <Text style={[styles.tabText, !isLogin && styles.tabTextActive]}>
            {t('auth.register')}
          </Text>
        </TouchableOpacity>
      </View>

      <View style={styles.form}>
        <Text style={styles.label}>{t('auth.email')}</Text>
        <TextInput
          style={styles.input}
          placeholder="email@example.com"
          placeholderTextColor={neutral400}
          keyboardType="email-address"
          autoCapitalize="none"
        />

        <Text style={styles.label}>{t('auth.password')}</Text>
        <TextInput
          style={styles.input}
          placeholder="••••••••"
          placeholderTextColor={neutral400}
          secureTextEntry
        />

        <TouchableOpacity style={styles.submitButton}>
          <Text style={styles.submitButtonText}>
            {isLogin ? t('auth.loginButton') : t('auth.registerButton')}
          </Text>
        </TouchableOpacity>

        <View style={styles.socialSection}>
          <TouchableOpacity style={styles.socialButton}>
            <Text style={styles.socialButtonText}>
              {t('auth.loginWithGoogle')}
            </Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.socialButton}>
            <Text style={styles.socialButtonText}>
              {t('auth.loginWithApple')}
            </Text>
          </TouchableOpacity>
          <TouchableOpacity style={styles.socialButton}>
            <Text style={styles.socialButtonText}>
              {t('auth.loginWithFacebook')}
            </Text>
          </TouchableOpacity>
        </View>

        <TouchableOpacity onPress={() => setIsLogin(!isLogin)}>
          <Text style={styles.switchText}>
            {isLogin ? t('auth.noAccount') : t('auth.hasAccount')}
          </Text>
        </TouchableOpacity>
      </View>
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
  tabs: {
    flexDirection: 'row',
    marginBottom: spacing4,
    borderRadius: radiusMd,
    overflow: 'hidden',
    borderWidth: 1,
    borderColor: brandPrimary,
  },
  tab: {
    flex: 1,
    paddingVertical: spacing3,
    alignItems: 'center',
  },
  tabActive: {
    backgroundColor: brandPrimary,
  },
  tabText: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    fontWeight: fontWeightMedium,
    color: brandPrimary,
  },
  tabTextActive: {
    color: '#FFFFFF',
  },
  form: {
    gap: spacing3,
  },
  label: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: neutral600,
    marginBottom: spacing2,
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
  submitButton: {
    backgroundColor: brandPrimary,
    borderRadius: radiusLg,
    padding: spacing4,
    alignItems: 'center',
    marginTop: spacing2,
  },
  submitButtonText: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeBase,
    fontWeight: fontWeightMedium,
    color: '#FFFFFF',
  },
  socialSection: {
    marginTop: spacing4,
    gap: spacing2,
  },
  socialButton: {
    backgroundColor: neutral100,
    borderRadius: radiusMd,
    padding: spacing4,
    alignItems: 'center',
    borderWidth: 1,
    borderColor: neutral200,
  },
  socialButtonText: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeBase,
    color: neutral700,
  },
  switchText: {
    fontFamily: fontFamilySans,
    fontSize: fontSizeSm,
    color: brandPrimary,
    textAlign: 'center',
    marginTop: spacing4,
  },
})
