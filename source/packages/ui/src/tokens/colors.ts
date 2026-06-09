export const colors = {
  brand: {
    primary: '#007943',
    primaryDark: '#005c32',
    sageLight: '#EAF0E6',
    glow: '#00E676',
  },
  surface: {
    background: '#F8FAF6',
    card: '#FFFFFF',
    sidebar: '#FFFFFF',
    mapTerrain: '#EAF0E6',
  },
  text: {
    main: '#111827',
    muted: '#6B7280',
  },
  border: {
    default: '#E5E7EB',
    subtle: '#F3F4F6',
  },
  status: {
    available: '#10B981',
    availableBg: '#ECFDF5',
    inUse: '#F59E0B',
    inUseBg: '#FFFBEB',
    maintenance: '#EF4444',
    maintenanceBg: '#FEF2F2',
  },
  neutral: {
    50: '#F9FAFB',
    100: '#F3F4F6',
    200: '#E5E7EB',
    300: '#D1D5DB',
    400: '#9CA3AF',
    500: '#6B7280',
    600: '#4B5563',
    700: '#374151',
    800: '#1F2937',
    900: '#111827',
  },
} as const;

export type ColorTokens = typeof colors;
