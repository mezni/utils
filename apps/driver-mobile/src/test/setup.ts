import '@testing-library/jest-native/extend-expect'

jest.mock('expo-localization', () => ({
  __esModule: true,
  default: {
    getLocales: () => [{ languageCode: 'en' }],
  },
}))

jest.mock('i18next', () => ({
  __esModule: true,
  default: {
    use: jest.fn().mockReturnThis(),
    init: jest.fn().mockReturnThis(),
    t: (key: string) => key,
  },
  createInstance: jest.fn(() => ({
    use: jest.fn().mockReturnThis(),
    init: jest.fn().mockReturnThis(),
    t: (key: string) => key,
  })),
}))

jest.mock('react-i18next', () => ({
  __esModule: true,
  useTranslation: () => ({
    t: (key: string) => key,
    i18n: { language: 'en' },
  }),
}))

jest.mock('@react-navigation/native', () => ({
  __esModule: true,
  NavigationContainer: ({ children }: { children: React.ReactNode }) => children,
  useNavigation: () => ({
    navigate: jest.fn(),
    goBack: jest.fn(),
    setParams: jest.fn(),
  }),
}))

jest.mock('@react-navigation/bottom-tabs', () => ({
  __esModule: true,
  createBottomTabNavigator: () => ({
    Navigator: ({ children }: { children: React.ReactNode }) => children,
    Screen: () => null,
  }),
}))

jest.mock('@react-navigation/native-stack', () => ({
  __esModule: true,
  createNativeStackNavigator: () => ({
    Navigator: ({ children }: { children: React.ReactNode }) => children,
    Screen: () => null,
  }),
}))

jest.mock('expo-font', () => ({
  __esModule: true,
  useFonts: () => [true],
}))

jest.mock('expo-status-bar', () => ({
  __esModule: true,
  StatusBar: null,
}))
