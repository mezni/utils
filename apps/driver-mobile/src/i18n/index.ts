import i18n from 'i18next'
import { initReactI18next } from 'react-i18next'
import { getLocales } from 'expo-localization'
import { I18nManager } from 'react-native'
import ar from './ar.json'
import fr from './fr.json'

const deviceLanguage = getLocales()?.[0]?.languageCode ?? 'fr'

i18n
  .use(initReactI18next)
  .init({
    resources: {
      ar: { translation: ar },
      fr: { translation: fr },
    },
    lng: deviceLanguage,
    fallbackLng: 'fr',
    interpolation: {
      escapeValue: false,
    },
  })

if (deviceLanguage === 'ar' && !I18nManager.isRTL) {
  I18nManager.forceRTL(true)
}

export default i18n
