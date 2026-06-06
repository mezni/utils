import { useTranslation } from 'react-i18next'
import * as i18n from '../i18n'

export const useMockData = () => {
  const { t } = useTranslation()
  
  return {
    changeLanguage: (lang: string) => {
      i18n.default.changeLanguage(lang)
    },
    t
  }
}