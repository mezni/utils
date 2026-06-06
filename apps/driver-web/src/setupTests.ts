import '@testing-library/jest-dom'
import { vi } from 'vitest'

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string) => {
      const translations: Record<string, string> = {
        'app.name': 'BorneMap',
        'home.searchPlaceholder': 'Rechercher une station...',
        'home.noStations': 'Aucune station à proximité',
        'station.available': 'Disponible',
        'station.unavailable': 'Indisponible',
        'station.chargers': 'Chargeurs',
        'station.distance': 'km',
        'station.reviews': 'Avis',
        'station.noReviews': 'Aucun avis pour le moment',
        'station.noChargers': 'Aucun chargeur',
        'station.pricePerKwh': 'TND/kWh',
        'station.directions': 'Itinéraire',
        'charger.type2': 'Type 2',
        'charger.ccs': 'CCS',
        'charger.chademo': 'CHAdeMO',
        'search.title': 'Résultats de recherche',
        'search.noResults': 'Aucun résultat trouvé',
        'search.all': 'Tous',
        'search.available': 'Disponible uniquement',
        'favorites.title': 'Favoris',
        'favorites.empty': 'Aucune station en favori',
        'profile.title': 'Profil',
        'profile.name': 'Nom',
        'profile.email': 'Email',
        'profile.phone': 'Téléphone',
        'profile.save': 'Enregistrer',
        'auth.login': 'Connexion',
        'auth.register': 'Inscription',
        'auth.email': 'Email',
        'auth.password': 'Mot de passe',
        'auth.loginButton': 'Se connecter',
        'auth.registerButton': "S'inscrire",
        'auth.loginWithGoogle': 'Se connecter avec Google',
        'auth.loginWithApple': 'Se connecter avec Apple',
        'auth.loginWithFacebook': 'Se connecter avec Facebook',
        'auth.noAccount': 'Pas encore de compte ?',
        'auth.hasAccount': 'Déjà un compte ?',
        'common.error': 'Une erreur est survenue',
        'common.loading': 'Chargement...',
        'common.retry': 'Réessayer',
      }
      return translations[key] || key
    },
    i18n: { language: 'fr', changeLanguage: vi.fn() },
  }),
  initReactI18next: { type: '3rdParty' as const },
}))

vi.mock('../mocks/users', () => ({
  users: [{
    id: 'USR-001',
    name: 'Ahmed Ben Salem',
    email: 'ahmed.bensalem@example.tn',
    phone: '+216 52 123 456',
    avatarUrl: '',
    favoriteStationIds: ['STN-001', 'STN-003', 'STN-011'],
    language: 'fr',
  }],
}))
